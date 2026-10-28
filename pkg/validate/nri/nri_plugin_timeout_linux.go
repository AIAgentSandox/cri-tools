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
			"should not leak a sandbox when the RunPodSandbox hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
				// Spec: a plugin timeout is treated as an error; on RunPodSandbox
				// it may fail pod creation depending on runtime policy. Either
				// way the CRI call must return, and a failed call must not leave
				// a sandbox behind.
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

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "RunPodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"RunPodSandbox did not return within %v; the runtime MUST NOT wait on an "+
						"unresponsive plugin beyond its NRI plugin request timeout",
					pluginTimeoutCallTimeout)

				if runErr == nil {
					// Runtime policy: the plugin timeout did not fail pod
					// creation. containerd treats the timeout as a fatal plugin
					// error, closes the timed-out plugin and lets the CRI call
					// proceed. The sandbox must then be fully usable.
					podID = runPodID
					Expect(podID).NotTo(BeEmpty())

					framework.Logf(
						"RunPodSandbox succeeded despite the hook timeout (runtime policy): %s",
						podID,
					)

					statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(
						statusResp.GetStatus().GetState(),
					).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"sandbox created despite a hook timeout must be ready")

					return
				}

				framework.Logf("RunPodSandbox failed after the hook timeout: %v", runErr)
				Expect(runPodID).To(BeEmpty(),
					"No pod sandbox ID should be returned when RunPodSandbox fails")

				By("verifying the NRI RunPodSandbox hook actually fired")
				// The hook records its event before blocking, so the attempted
				// sandbox ID is available for the leak check below.
				attemptedID := testStub.Plugin.LastRunPodSandboxID()
				Expect(attemptedID).NotTo(BeEmpty(),
					"NRI RunPodSandbox hook should have fired before the failure")

				By("verifying the failed RunPodSandbox is not left behind")
				// The runtime may unwind the sandbox asynchronously after
				// returning the error, so poll instead of listing once.
				Eventually(func() []string {
					pods, listErr := rc.ListPodSandbox(ctx, nil)
					Expect(listErr).NotTo(HaveOccurred())

					var leaked []string

					for _, pod := range pods {
						if pod.GetId() == attemptedID ||
							pod.GetMetadata().GetName() == podSandboxName {
							leaked = append(leaked, pod.GetId())
						}
					}

					return leaked
				}, 30*time.Second, 500*time.Millisecond).Should(BeEmpty(),
					"a RunPodSandbox that failed due to a plugin timeout MUST NOT leave a sandbox behind")

				By("retrying RunPodSandbox once the plugin responds in time")

				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty(),
					"RunPodSandbox retry should succeed once the plugin responds in time")

				By("verifying the retried sandbox becomes Ready")

				statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
				Expect(statusErr).NotTo(HaveOccurred())
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"sandbox should become Ready after a successful RunPodSandbox retry")
			},
		)

		It(
			"should not leak a container when the CreateContainer hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
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

				// Release the hook so the stub no longer holds up the runtime.
				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "CreateContainer NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"CreateContainer did not return within %v; the runtime MUST NOT wait on an "+
						"unresponsive plugin beyond its NRI plugin request timeout",
					pluginTimeoutCallTimeout)

				if createErr == nil {
					// Runtime policy: the plugin timeout did not fail container
					// creation (see the RunPodSandbox spec above).
					containerID = createdID
					Expect(containerID).NotTo(BeEmpty())

					framework.Logf(
						"CreateContainer succeeded despite the hook timeout (runtime policy): %s",
						containerID,
					)

					statusResp, statusErr := rc.ContainerStatus(ctx, containerID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(
						statusResp.GetStatus().GetState(),
					).To(Equal(runtimeapi.ContainerState_CONTAINER_CREATED),
						"container created despite a hook timeout must be in CREATED state")

					return
				}

				framework.Logf("CreateContainer failed after the hook timeout: %v", createErr)
				Expect(createdID).To(BeEmpty(),
					"No container ID should be returned when CreateContainer fails")

				By("verifying the failed CreateContainer leaked no container")

				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(listErr).NotTo(HaveOccurred(), "ListContainers after failed CreateContainer")

				for _, c := range containers {
					if c.GetMetadata().GetName() == containerName {
						containerID = c.GetId() // hand to AfterEach for cleanup
						Fail(
							fmt.Sprintf(
								"container %s was leaked after a CreateContainer that timed out on the plugin",
								c.GetId(),
							),
						)
					}
				}

				By("retrying CreateContainer with the same name")

				containerID = framework.CreateContainer(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(containerID).NotTo(BeEmpty(),
					"CreateContainer retry with the same name should succeed once the plugin responds in time")
			},
		)

		It(
			"should stop and remove the sandbox when the StopPodSandbox hook exceeds the request timeout",
			func(ctx SpecContext) {
				// Spec: StopPodSandbox is non-blocking with respect to plugin
				// timeouts; a plugin error MUST NOT prevent the sandbox from
				// being stopped.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-stop", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox")

				podConfig = pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-stop-"+framework.NewUUID(),
				)
				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

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
					"StopPodSandbox did not return within %v; the runtime MUST NOT wait on an "+
						"unresponsive plugin beyond its NRI plugin request timeout",
					pluginTimeoutCallTimeout)
				Expect(stopErr).NotTo(HaveOccurred(),
					"a StopPodSandbox hook timeout MUST NOT prevent the sandbox from being stopped")

				statusResp, statusErr := rc.PodSandboxStatus(ctx, podID, false)
				Expect(statusErr).NotTo(HaveOccurred())
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"sandbox MUST be stopped after StopPodSandbox returns")

				By("removing the sandbox")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after a StopPodSandbox hook timeout")

				removedID := podID
				// Clear podID so AfterEach does not stop/remove it again.
				podID, podRemoved = "", true

				pods, listErr := rc.ListPodSandbox(ctx, nil)
				Expect(listErr).NotTo(HaveOccurred())

				for _, pod := range pods {
					Expect(pod.GetId()).NotTo(Equal(removedID), "sandbox MUST be removed")
				}
			},
		)

		It(
			"should remove the sandbox when the RemovePodSandbox hook exceeds the request timeout",
			func(ctx SpecContext) {
				// Spec: RemovePodSandbox is non-blocking; removal proceeds
				// regardless of a plugin timeout.
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
					"RemovePodSandbox did not return within %v; the runtime MUST NOT wait on an "+
						"unresponsive plugin beyond its NRI plugin request timeout",
					pluginTimeoutCallTimeout)
				Expect(removeErr).NotTo(HaveOccurred(),
					"a RemovePodSandbox hook timeout MUST NOT prevent the sandbox from being removed")

				removedID := podID
				// Clear podID so AfterEach does not stop/remove it again.
				podID, podRemoved = "", true

				pods, listErr := rc.ListPodSandbox(ctx, nil)
				Expect(listErr).NotTo(HaveOccurred())

				for _, pod := range pods {
					Expect(pod.GetId()).NotTo(Equal(removedID), "sandbox MUST be removed")
				}
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
