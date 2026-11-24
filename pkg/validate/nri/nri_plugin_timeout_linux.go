/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nri

import (
	"context"
	"fmt"
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

const (
	// pluginTimeoutCallTimeout bounds how long a CRI call may take while an
	// NRI hook never responds. Runtimes enforce a much smaller NRI plugin
	// request timeout (containerd's plugin_request_timeout defaults to 2s),
	// so 30s leaves ample room for that timeout plus the non-NRI work of the
	// call (sandbox setup and teardown, container creation). A call still
	// pending after that means the runtime is waiting on the plugin instead of
	// enforcing its own timeout.
	pluginTimeoutCallTimeout = 30 * time.Second

	// pluginTimeoutSlowHookDelay is how long the "slow but in time" hook
	// delays its reply. It must stay below the NRI plugin request timeout of
	// every runtime under test (containerd: 2s), so that the runtime does not
	// time the hook out.
	pluginTimeoutSlowHookDelay = time.Second

	// pluginTimeoutDisconnectWait is how long the specs wait, after a CRI
	// call returns, for the stub to notice that the runtime disconnected it.
	pluginTimeoutDisconnectWait = 5 * time.Second

	// pluginTimeoutHookSafetyBound caps how long a blocking hook waits for the
	// test to release it, so a hook can never outlive the spec indefinitely
	// even if AfterEach is not reached.
	pluginTimeoutHookSafetyBound = 3 * time.Minute
)

// pluginTimeoutGate blocks the first hook invocation routed through it until
// released, simulating an unresponsive plugin. It deliberately ignores the
// hook context so that the runtime, not the plugin, must enforce the request
// timeout. Subsequent invocations (e.g. retries, or a redelivered idempotent
// stop) pass through immediately, matching the sync.Once guard the other NRI
// contract specs use around their blocking hooks.
type pluginTimeoutGate struct {
	first       sync.Once
	releaseOnce sync.Once
	reached     chan struct{}
	release     chan struct{}
}

func newPluginTimeoutGate() *pluginTimeoutGate {
	return &pluginTimeoutGate{
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *pluginTimeoutGate) block() error {
	isFirst := false

	g.first.Do(func() { isFirst = true })

	if !isFirst {
		return nil
	}

	close(g.reached)

	select {
	case <-g.release:
	case <-time.After(pluginTimeoutHookSafetyBound):
	}

	return nil
}

func (g *pluginTimeoutGate) wasReached() bool {
	select {
	case <-g.reached:
		return true
	default:
		return false
	}
}

func (g *pluginTimeoutGate) unblock() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// pluginTimeoutPodConfig builds a pod sandbox config for the given sandbox name.
func pluginTimeoutPodConfig(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podSandboxName string,
) *runtimeapi.PodSandboxConfig {
	return &runtimeapi.PodSandboxConfig{
		Metadata: framework.BuildPodSandboxMetadata(
			podSandboxName,
			framework.DefaultUIDPrefix+framework.NewUUID(),
			framework.DefaultNamespacePrefix+framework.NewUUID(),
			framework.DefaultAttempt,
		),
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			CgroupParent: common.GetCgroupParent(ctx, rc),
		},
		Labels: framework.DefaultPodLabels,
	}
}

// pluginTimeoutDisconnected reports whether the runtime disconnected the stub,
// waiting up to pluginTimeoutDisconnectWait for it to notice. The stub's Run
// loop exits when the runtime closes the plugin connection, so this is
// observed independently of the CRI call's result.
func pluginTimeoutDisconnected(ts *NRITestStub) bool {
	select {
	case <-ts.done:
		return true
	case <-time.After(pluginTimeoutDisconnectWait):
		return false
	}
}

// pluginTimeoutExpectRollback asserts that events contains, in this order, an
// event of each of the given types matching the rolled-back object. Repeats
// are allowed: a failed create is not a successful operation, so a rollback
// teardown may be delivered more than once.
func pluginTimeoutExpectRollback(
	events []NRIEvent,
	match func(NRIEvent) bool,
	object string,
	types ...NRIEventType,
) {
	pos := 0

	for n, eventType := range types {
		found := -1

		for i := pos; i < len(events); i++ {
			if events[i].Type == eventType && match(events[i]) {
				found = i

				break
			}
		}

		Expect(found).To(BeNumerically(">=", 0),
			"rollback of %s: expected %v after %v by the time the CRI call returned; "+
				"a plugin whose create hook was entered must get the matching teardown "+
				"when the create fails, or it leaks what it allocated (events: %+v)",
			object, eventType, types[:n], events)

		pos = found + 1
	}
}

// pluginTimeoutCreateDeviation is the Skip note for a create that succeeded
// after its NRI hook exceeded the request timeout.
func pluginTimeoutCreateDeviation(call, objectID string) string {
	return fmt.Sprintf(
		"%[1]s succeeded (%[2]s) although the NRI plugin exceeded the request timeout. "+
			"Expected: a plugin that exceeds the request timeout on RunPodSandbox/CreateContainer "+
			"fails the CRI call like a plugin error; nothing is left behind; the runtime may also "+
			"disconnect the plugin. Reason: otherwise a pod starts without setup a plugin may need, "+
			"such as networking or devices. Observed: the call succeeded, the plugin was evicted "+
			"(disconnected) and the timeout error was dropped. Related upstream discussion: "+
			"https://github.com/containerd/nri/issues/114",
		call, objectID,
	)
}

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var (
		rc internalapi.RuntimeService
		ic internalapi.ImageManagerService
	)

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
		ic = f.CRIClient.CRIImageClient
	})

	Context("plugin timeout", Serial, func() {
		var (
			testStub    *NRITestStub
			gate        *pluginTimeoutGate
			podID       string
			podConfig   *runtimeapi.PodSandboxConfig
			containerID string
			// podRemoved records that the spec already removed its sandbox, so
			// AfterEach does not fall back to the hook-recorded ID and try again.
			podRemoved bool
		)

		BeforeEach(func() {
			gate = newPluginTimeoutGate()
			podID = ""
			containerID = ""
			podRemoved = false
		})

		AfterEach(func(ctx SpecContext) {
			// Release any hook still blocked, then capture the fallback
			// sandbox ID before cleanup resets the recorded events: a CRI call
			// that failed on a hook timeout never returned an ID, so the hook
			// event is the only record of the sandbox the runtime attempted.
			gate.unblock()

			cleanupID := podID
			if cleanupID == "" && !podRemoved && testStub != nil {
				cleanupID = testStub.Plugin.LastRunPodSandboxID()
			}

			// Stop the stub first so the cleanup CRI calls below cannot be
			// held up by a hook that is still unresponsive.
			if testStub != nil {
				testStub.Cleanup()
				testStub = nil
			}

			if containerID != "" {
				if err := rc.StopContainer(ctx, containerID, 0); err != nil {
					framework.Logf("AfterEach: StopContainer(%s) failed: %v", containerID, err)
				}

				if err := rc.RemoveContainer(ctx, containerID); err != nil {
					framework.Logf("AfterEach: RemoveContainer(%s) failed: %v", containerID, err)
				}
			}

			if cleanupID != "" {
				if err := rc.StopPodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", cleanupID, err)
				}

				if err := rc.RemovePodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", cleanupID, err)
				}
			}
		})

		It(
			"should fail RunPodSandbox without leaking a sandbox when the RunPodSandbox hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
				// A plugin that exceeds the request timeout on RunPodSandbox
				// fails the call exactly like a plugin error, so that a pod
				// never starts without setup a plugin may need, such as
				// networking or devices (related: containerd/nri#114). The
				// runtime may also disconnect the plugin.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-run", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				testStub.Plugin.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				By("building the pod sandbox config")

				podSandboxName := "nri-test-timeout-run-" + framework.NewUUID()
				podConfig = pluginTimeoutPodConfig(ctx, rc, podSandboxName)

				By("running a pod sandbox while the RunPodSandbox hook stops responding")

				callCtx, cancel := context.WithTimeout(ctx, pluginTimeoutCallTimeout)
				defer cancel()

				runPodID, runErr := rc.RunPodSandbox(
					callCtx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				callTimedOut := callCtx.Err() != nil

				// Snapshot the events at return: rollback teardown must
				// already have been delivered by then.
				eventsAtReturn := testStub.Plugin.Events()

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "RunPodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"RunPodSandbox was still pending after %v with an unresponsive NRI hook; "+
						"the runtime must time the plugin out instead of waiting on it", pluginTimeoutCallTimeout)

				disconnected := pluginTimeoutDisconnected(testStub)
				framework.Logf(
					"plugin disconnected by the runtime after the RunPodSandbox hook timeout: %v",
					disconnected,
				)

				if runErr == nil {
					podID = runPodID
					Expect(podID).NotTo(BeEmpty())
					Expect(disconnected).To(BeTrue(),
						"RunPodSandbox succeeded after the hook timeout while the plugin is still connected")

					statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(statusResp.GetStatus().GetState()).To(
						Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"a sandbox returned by a successful RunPodSandbox must be Ready")

					By("cleaning up the sandbox before recording the deviation")
					Expect(rc.StopPodSandbox(ctx, podID)).To(Succeed())
					Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed())

					createdID := podID
					podID, podRemoved = "", true

					Skip(pluginTimeoutCreateDeviation("RunPodSandbox", createdID))
				}

				framework.Logf("RunPodSandbox failed after the hook timeout: %v", runErr)
				Expect(runPodID).To(BeEmpty(),
					"No pod sandbox ID should be returned when RunPodSandbox fails")

				By("verifying the NRI RunPodSandbox hook actually fired")
				// The hook records its event before blocking, so the attempted
				// sandbox ID is available for the checks below.
				attemptedID := testStub.Plugin.LastRunPodSandboxID()
				Expect(attemptedID).NotTo(BeEmpty(),
					"NRI RunPodSandbox hook should have fired before the failure")

				By("verifying the failed sandbox is not listed when RunPodSandbox returns")
				// The rollback is synchronous, so check once, right away.
				pods, listErr := rc.ListPodSandbox(ctx, nil)
				Expect(listErr).NotTo(HaveOccurred())

				for _, pod := range pods {
					matches := pod.GetId() == attemptedID ||
						pod.GetMetadata().GetName() == podSandboxName
					Expect(matches).To(BeFalse(),
						"sandbox %s MUST NOT be listed after a RunPodSandbox that failed on a plugin timeout",
						pod.GetId())
				}

				if disconnected {
					framework.Logf("plugin was disconnected; it is reconciled by Synchronize, " +
						"not by rollback teardown hooks")
				} else {
					By("verifying the still-connected plugin got the rollback teardown")
					pluginTimeoutExpectRollback(eventsAtReturn,
						func(e NRIEvent) bool { return e.PodSandboxID == attemptedID },
						"sandbox "+attemptedID,
						EventRunPodSandbox, EventStopPodSandbox, EventRemovePodSandbox)
				}

				By("retrying RunPodSandbox with the same metadata")

				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty(),
					"an immediate RunPodSandbox retry with the same metadata must succeed "+
						"after the timed-out attempt was rolled back")

				statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
				Expect(statusErr).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"sandbox should become Ready after a successful RunPodSandbox retry")

				By("cleaning up the retried sandbox")
				Expect(rc.StopPodSandbox(ctx, podID)).To(Succeed())
				Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed())

				podID, podRemoved = "", true
			},
		)

		It(
			"should fail CreateContainer without leaking a container when the CreateContainer hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
				// A plugin that exceeds the request timeout on CreateContainer
				// fails the call exactly like a plugin error, so that a
				// container never starts without setup a plugin may need, such
				// as devices (related: containerd/nri#114). The runtime may
				// also disconnect the plugin.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-create", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox")

				podConfig = pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-create-"+framework.NewUUID(),
				)
				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				containerName := "nri-test-timeout-create-ctr-" + framework.NewUUID()
				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						containerName,
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				testStub.Plugin.OnCreateContainer = func(
					_ context.Context, _ *nri.PodSandbox, _ *nri.Container,
				) error {
					return gate.block()
				}

				By("creating a container while the CreateContainer hook stops responding")

				callCtx, cancel := context.WithTimeout(ctx, pluginTimeoutCallTimeout)
				defer cancel()

				createdID, createErr := framework.CreateContainerWithError(
					callCtx, rc, ic, containerConfig, podID, podConfig,
				)
				callTimedOut := callCtx.Err() != nil

				// Snapshot the events at return: rollback teardown must
				// already have been delivered by then.
				eventsAtReturn := testStub.Plugin.Events()

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "CreateContainer NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"CreateContainer was still pending after %v with an unresponsive NRI hook; "+
						"the runtime must time the plugin out instead of waiting on it", pluginTimeoutCallTimeout)

				disconnected := pluginTimeoutDisconnected(testStub)
				framework.Logf(
					"plugin disconnected by the runtime after the CreateContainer hook timeout: %v",
					disconnected,
				)

				if createErr == nil {
					containerID = createdID
					Expect(containerID).NotTo(BeEmpty())
					Expect(disconnected).To(BeTrue(),
						"CreateContainer succeeded after the hook timeout while the plugin is still connected")

					statusResp, statusErr := rc.ContainerStatus(ctx, containerID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(statusResp.GetStatus().GetState()).To(
						Equal(runtimeapi.ContainerState_CONTAINER_CREATED),
						"a container returned by a successful CreateContainer must be in CREATED state")

					By("cleaning up the container before recording the deviation")
					Expect(rc.RemoveContainer(ctx, containerID)).To(Succeed())

					removedID := containerID
					containerID = ""

					Skip(pluginTimeoutCreateDeviation("CreateContainer", removedID))
				}

				framework.Logf("CreateContainer failed after the hook timeout: %v", createErr)
				Expect(createdID).To(BeEmpty(),
					"No container ID should be returned when CreateContainer fails")

				By("verifying the NRI CreateContainer hook actually fired")

				attemptedID := ""

				for _, e := range eventsAtReturn {
					if e.Type == EventCreateContainer && e.ContainerName == containerName {
						attemptedID = e.ContainerID
					}
				}

				Expect(attemptedID).NotTo(BeEmpty(),
					"NRI CreateContainer hook should have fired before the failure")

				By("verifying the failed container is not listed when CreateContainer returns")
				// The rollback is synchronous, so check once, right away.
				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(listErr).NotTo(HaveOccurred(), "ListContainers after failed CreateContainer")

				for _, c := range containers {
					if c.GetId() == attemptedID || c.GetMetadata().GetName() == containerName {
						containerID = c.GetId() // hand to AfterEach for cleanup
						Fail(fmt.Sprintf(
							"container %s MUST NOT be listed after a CreateContainer that failed on a plugin timeout",
							c.GetId(),
						))
					}
				}

				if disconnected {
					framework.Logf("plugin was disconnected; it is reconciled by Synchronize, " +
						"not by rollback teardown hooks")
				} else {
					By("verifying the still-connected plugin got the rollback teardown")
					// StopContainer is allowed before RemoveContainer but not required.
					pluginTimeoutExpectRollback(eventsAtReturn,
						func(e NRIEvent) bool { return e.ContainerID == attemptedID },
						"container "+attemptedID,
						EventCreateContainer, EventRemoveContainer)
				}

				By("retrying CreateContainer with the same name")

				containerID = framework.CreateContainer(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(containerID).NotTo(BeEmpty(),
					"an immediate CreateContainer retry with the same name must succeed "+
						"after the timed-out attempt was rolled back")

				By("cleaning up the retried container")
				Expect(rc.RemoveContainer(ctx, containerID)).To(Succeed())

				containerID = ""
			},
		)

		It(
			"should stop the sandbox and either report the StopPodSandbox hook timeout or disconnect the plugin",
			func(ctx SpecContext) {
				// A plugin failure, including a timeout, never blocks teardown
				// (containerd/nri#286 at 2339aa3, an unmerged draft: teardown
				// errors must not block). The timeout is reported so that
				// kubelet retries and the plugin gets the hook again, unless the
				// runtime disconnects the plugin for it, in which case the
				// plugin is reconciled by Synchronize instead.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-stop", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox with a running container")

				podConfig = pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-stop-"+framework.NewUUID(),
				)
				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				containerID = framework.CreateContainer(ctx, rc, ic, &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						"nri-test-timeout-stop-ctr-"+framework.NewUUID(),
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}, podID, podConfig)
				Expect(rc.StartContainer(ctx, containerID)).To(Succeed())

				testStub.Plugin.OnStopPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				By("stopping the sandbox while the StopPodSandbox hook stops responding")

				callCtx, cancel := context.WithTimeout(ctx, pluginTimeoutCallTimeout)
				defer cancel()

				stopErr := rc.StopPodSandbox(callCtx, podID)
				callTimedOut := callCtx.Err() != nil

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "StopPodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"StopPodSandbox was still pending after %v with an unresponsive NRI hook; "+
						"the runtime must time the plugin out instead of waiting on it", pluginTimeoutCallTimeout)

				disconnected := pluginTimeoutDisconnected(testStub)

				By("verifying the runtime stopped the sandbox and its container")

				statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
				Expect(statusErr).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"a StopPodSandbox hook timeout MUST NOT keep the sandbox from being stopped")

				ctrResp, ctrErr := rc.ContainerStatus(ctx, containerID, false)
				Expect(ctrErr).NotTo(HaveOccurred())
				Expect(ctrResp.GetStatus().GetState()).To(
					Equal(runtimeapi.ContainerState_CONTAINER_EXITED),
					"a StopPodSandbox hook timeout MUST NOT keep the sandbox's containers running")

				if stopErr != nil {
					framework.Logf(
						"StopPodSandbox reported the hook timeout and kept the plugin connected: %v",
						stopErr,
					)
					Expect(disconnected).To(BeFalse(),
						"StopPodSandbox failed although the timed-out plugin was disconnected; "+
							"a disconnected plugin is no longer pending, so the call should succeed")

					By("retrying StopPodSandbox and verifying the hook is redelivered")
					Expect(rc.StopPodSandbox(ctx, podID)).To(Succeed(),
						"StopPodSandbox retry should succeed once the plugin responds")

					stops := 0

					for _, e := range FilterEventsByPodID(testStub.Plugin.Events(), podID) {
						if e.Type == EventStopPodSandbox {
							stops++
						}
					}

					Expect(stops).To(BeNumerically(">=", 2),
						"a StopPodSandbox retry after a failed hook must redeliver the hook to the plugin")
				} else {
					framework.Logf(
						"StopPodSandbox succeeded and the runtime disconnected the timed-out plugin",
					)
					Expect(disconnected).To(BeTrue(),
						"StopPodSandbox succeeded although the timed-out plugin is still connected; "+
							"the timeout must be reported as an error so that kubelet retries "+
							"and the plugin gets the hook again")
				}

				By("removing the sandbox")
				Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed(),
					"RemovePodSandbox should succeed after a StopPodSandbox hook timeout")

				removedID := podID
				// Clear the IDs so AfterEach does not stop/remove them again.
				podID, podRemoved, containerID = "", true, ""

				pods, listErr := rc.ListPodSandbox(ctx, nil)
				Expect(listErr).NotTo(HaveOccurred())

				for _, pod := range pods {
					Expect(pod.GetId()).NotTo(Equal(removedID), "sandbox MUST be removed")
				}
			},
		)

		It(
			"should either withhold removal and report the RemovePodSandbox hook timeout or disconnect the plugin and remove the sandbox",
			func(ctx SpecContext) {
				// A plugin failure, including a timeout, never blocks teardown
				// (containerd/nri#286 at 2339aa3, an unmerged draft). While a
				// connected plugin has not acknowledged RemovePodSandbox, the
				// sandbox stays listed and the call fails, so that the plugin is
				// told again on retry and does not leak; a plugin the runtime
				// disconnected is reconciled by Synchronize instead.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-remove", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating and stopping a pod sandbox")

				podConfig = pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-remove-"+framework.NewUUID(),
				)
				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())
				Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())

				testStub.Plugin.OnRemovePodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				By("removing the sandbox while the RemovePodSandbox hook stops responding")

				callCtx, cancel := context.WithTimeout(ctx, pluginTimeoutCallTimeout)
				defer cancel()

				removeErr := rc.RemovePodSandbox(callCtx, podID)
				callTimedOut := callCtx.Err() != nil

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "RemovePodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"RemovePodSandbox was still pending after %v with an unresponsive NRI hook; "+
						"the runtime must time the plugin out instead of waiting on it", pluginTimeoutCallTimeout)

				disconnected := pluginTimeoutDisconnected(testStub)

				sandboxListed := func() *runtimeapi.PodSandbox {
					pods, listErr := rc.ListPodSandbox(ctx, nil)
					Expect(listErr).NotTo(HaveOccurred())

					for _, pod := range pods {
						if pod.GetId() == podID {
							return pod
						}
					}

					return nil
				}

				if removeErr != nil {
					framework.Logf(
						"RemovePodSandbox reported the hook timeout and kept the plugin connected: %v",
						removeErr,
					)
					Expect(disconnected).To(BeFalse(),
						"RemovePodSandbox failed although the timed-out plugin was disconnected; "+
							"a disconnected plugin is no longer pending, so the call should succeed")

					By(
						"verifying the sandbox is withheld while the plugin has not acknowledged removal",
					)

					pod := sandboxListed()
					Expect(pod).NotTo(BeNil(),
						"the sandbox MUST stay listed while a connected plugin has not acknowledged RemovePodSandbox")
					Expect(pod.GetState()).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY))

					By("retrying RemovePodSandbox and verifying the hook is redelivered")
					Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed(),
						"RemovePodSandbox retry should succeed once the plugin responds")

					removes := 0

					for _, e := range FilterEventsByPodID(testStub.Plugin.Events(), podID) {
						if e.Type == EventRemovePodSandbox {
							removes++
						}
					}

					Expect(removes).To(BeNumerically(">=", 2),
						"a RemovePodSandbox retry after a failed hook must redeliver the hook to the plugin")
				} else {
					framework.Logf(
						"RemovePodSandbox succeeded and the runtime disconnected the timed-out plugin",
					)
					Expect(disconnected).To(BeTrue(),
						"RemovePodSandbox succeeded although the timed-out plugin is still connected; "+
							"the timeout must be reported as an error so that kubelet retries "+
							"and the plugin gets the hook again")
				}

				Expect(sandboxListed()).To(BeNil(), "sandbox MUST be removed")

				// Clear podID so AfterEach does not stop/remove it again.
				podID, podRemoved = "", true
			},
		)

		It(
			"should deliver StopPodSandbox to a plugin whose RunPodSandbox hook was slow but within the request timeout",
			func(ctx SpecContext) {
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-slow-run", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
					select {
					case <-time.After(pluginTimeoutSlowHookDelay):
					case <-hookCtx.Done():
					}

					return nil
				}

				By("running a pod sandbox with a slow but in-time RunPodSandbox hook")

				podConfig = pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-slow-run-"+framework.NewUUID(),
				)
				start := time.Now()
				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())
				Expect(time.Since(start)).To(BeNumerically(">=", pluginTimeoutSlowHookDelay),
					"RunPodSandbox should wait for the in-time hook to complete")

				By("stopping the sandbox")
				Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())

				By("verifying the plugin received StopPodSandbox for the sandbox")
				Eventually(func() []NRIEventType {
					events := FilterEventsByPodID(testStub.Plugin.Events(), podID)

					types := make([]NRIEventType, 0, len(events))
					for _, e := range events {
						types = append(types, e.Type)
					}

					return types
				}, 10*time.Second, 50*time.Millisecond).Should(ContainElements(
					EventRunPodSandbox,
					EventStopPodSandbox,
				), "a plugin whose RunPodSandbox hook completed in time MUST still receive StopPodSandbox")
			},
		)
	})
})
