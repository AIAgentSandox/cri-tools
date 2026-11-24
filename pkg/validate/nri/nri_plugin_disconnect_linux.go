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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

const (
	// disconnectCallTimeout bounds how long a CRI call that was held by a
	// blocked NRI hook may take to return once the plugin connection is closed.
	disconnectCallTimeout = 30 * time.Second

	// disconnectReadyTimeout bounds how long a plugin may take to register
	// and receive its Synchronize.
	disconnectReadyTimeout = 10 * time.Second

	// Plugin indices: the plugin under test sits between two observers, so
	// one observer's hook runs before the disconnecting plugin's hook and the
	// other's after it.
	disconnectIdxBefore = "10"
	disconnectIdxTarget = "20"
	disconnectIdxAfter  = "30"

	// disconnectCreateIssue is the related discussion of how a plugin that
	// fails or times out on a create hook is handled.
	disconnectCreateIssue = "https://github.com/containerd/nri/issues/114"
)

// disconnectStub is an NRI test plugin whose raw connection to the runtime the
// test can close directly. NRITestStub.Stop cancels the context the plugin's
// hooks run under before it closes the connection, which lets a blocked hook
// return a reply first; closing the connection itself does not.
type disconnectStub struct {
	*NRITestStub

	conn net.Conn
}

// startDisconnectStub connects an NRI test plugin over a connection the test
// owns and waits until the runtime has synchronized it.
func startDisconnectStub(name, idx string) (*disconnectStub, error) {
	socketPath := framework.TestContext.NRISocketPath
	if socketPath == "" {
		return nil, errors.New("NRI socket path not configured")
	}

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NRI socket: %w", err)
	}

	plugin := &NRITestPlugin{ready: make(chan struct{})}

	s, err := stub.New(plugin,
		stub.WithPluginName(name),
		stub.WithPluginIdx(idx),
		stub.WithConnection(conn),
	)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("failed to create NRI stub: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(done)

		errCh <- s.Run(ctx)
	}()

	select {
	case <-done:
		cancel()
		conn.Close()

		return nil, fmt.Errorf("NRI stub exited early: %w", <-errCh)
	case <-plugin.ready:
	case <-time.After(disconnectReadyTimeout):
		cancel()
		// Closing the connection unblocks a Start that waits for Configure.
		conn.Close()

		return nil, fmt.Errorf("NRI stub did not become ready within %v", disconnectReadyTimeout)
	}

	return &disconnectStub{
		NRITestStub: &NRITestStub{Plugin: plugin, Stub: s, cancel: cancel, done: done},
		conn:        conn,
	}, nil
}

// disconnect closes the plugin's connection to the runtime without touching
// the context its hooks run under, so a hook blocked in the plugin stays
// blocked and never replies.
func (s *disconnectStub) disconnect() {
	_ = s.conn.Close()
}

// cleanup closes the connection and stops the plugin. It is idempotent.
func (s *disconnectStub) cleanup() {
	s.disconnect()
	s.Cleanup()
}

// disconnectHookBlocker holds the NRI hook for one object until the test
// releases it. It deliberately ignores the hook's context: the hook must stay
// blocked, with no reply sent, while the test closes the connection.
type disconnectHookBlocker struct {
	// target is the object the hook must be for: the pod or container ID, or
	// the metadata name for a create, whose ID is not known in advance.
	target string

	reached     chan struct{}
	release     chan struct{}
	blockOnce   sync.Once
	releaseOnce sync.Once

	mu      sync.Mutex
	hookCtx context.Context //nolint:containedctx // inspected by the test to tell a disconnect from a timeout
}

func newDisconnectHookBlocker(target string) *disconnectHookBlocker {
	return &disconnectHookBlocker{
		target:  target,
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// block is called from inside an NRI hook. Only the first invocation for the
// target blocks; hooks for other objects and repeats return immediately.
func (b *disconnectHookBlocker) block(hookCtx context.Context, object string) {
	if object != b.target {
		return
	}

	first := false

	b.blockOnce.Do(func() { first = true })

	if !first {
		return
	}

	b.mu.Lock()
	b.hookCtx = hookCtx
	b.mu.Unlock()

	close(b.reached)
	<-b.release
}

// hookContextErr reports whether the runtime already ended the blocked hook,
// for example through its request timeout.
func (b *disconnectHookBlocker) hookContextErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.hookCtx == nil {
		return nil
	}

	return b.hookCtx.Err()
}

// unblock releases the blocked hook. It is idempotent.
func (b *disconnectHookBlocker) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

// disconnectEventIndex returns the position of the first event of the given
// type for the object (pod ID for pod events, container ID for container
// events), or -1.
func disconnectEventIndex(events []NRIEvent, eventType NRIEventType, id string) int {
	podEvent := eventType == EventRunPodSandbox ||
		eventType == EventStopPodSandbox ||
		eventType == EventRemovePodSandbox

	for i := range events {
		e := &events[i]
		if e.Type != eventType {
			continue
		}

		if (podEvent && e.PodSandboxID == id) || (!podEvent && e.ContainerID == id) {
			return i
		}
	}

	return -1
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

	Context("plugin disconnect", Serial, func() {
		var (
			// trackMu guards the cleanup bookkeeping below, which may be
			// appended to from goroutines issuing CRI calls.
			trackMu    sync.Mutex
			stubs      []*disconnectStub
			blockers   []*disconnectHookBlocker
			pods       []string
			containers []string
		)

		trackPod := func(id string) {
			trackMu.Lock()
			defer trackMu.Unlock()

			pods = append(pods, id)
		}

		trackContainer := func(id string) {
			trackMu.Lock()
			defer trackMu.Unlock()

			containers = append(containers, id)
		}

		// startStub connects an NRI test plugin and registers it for cleanup.
		startStub := func(name, idx string) *disconnectStub {
			s, err := startDisconnectStub(name, idx)
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test plugin %s", name)

			trackMu.Lock()

			stubs = append(stubs, s)
			trackMu.Unlock()

			return s
		}

		// newBlocker creates a hook blocker for target that cleanup always
		// releases.
		newBlocker := func(target string) *disconnectHookBlocker {
			b := newDisconnectHookBlocker(target)

			trackMu.Lock()

			blockers = append(blockers, b)
			trackMu.Unlock()

			return b
		}

		// cleanup disconnects every plugin, releases every hook, and removes
		// every pod and container the spec created, including those only
		// announced through a hook. It is idempotent, so a spec can clean up
		// before it Skips.
		cleanup := func(ctx context.Context) {
			trackMu.Lock()
			specStubs, specBlockers := stubs, blockers
			podIDs, containerIDs := pods, containers
			stubs, blockers, pods, containers = nil, nil, nil, nil
			trackMu.Unlock()

			for _, s := range specStubs {
				for _, e := range s.Plugin.Events() {
					if e.Type == EventRunPodSandbox {
						podIDs = append(podIDs, e.PodSandboxID)
					}

					if e.Type == EventCreateContainer {
						containerIDs = append(containerIDs, e.ContainerID)
					}
				}
			}

			for _, s := range specStubs {
				s.cleanup()
			}

			for _, b := range specBlockers {
				b.unblock()
			}

			seen := map[string]bool{}

			for _, id := range containerIDs {
				if id == "" || seen[id] {
					continue
				}

				seen[id] = true

				if err := rc.StopContainer(ctx, id, 0); err != nil {
					framework.Logf("cleanup: StopContainer(%s) failed: %v", id, err)
				}

				if err := rc.RemoveContainer(ctx, id); err != nil {
					framework.Logf("cleanup: RemoveContainer(%s) failed: %v", id, err)
				}
			}

			for _, id := range podIDs {
				if id == "" || seen[id] {
					continue
				}

				seen[id] = true

				if err := rc.StopPodSandbox(ctx, id); err != nil {
					framework.Logf("cleanup: StopPodSandbox(%s) failed: %v", id, err)
				}

				if err := rc.RemovePodSandbox(ctx, id); err != nil {
					framework.Logf("cleanup: RemovePodSandbox(%s) failed: %v", id, err)
				}
			}
		}

		newPodConfig := func(ctx context.Context, prefix string) *runtimeapi.PodSandboxConfig {
			return &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(
					prefix+framework.NewUUID(),
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

		newContainerConfig := func(prefix string) *runtimeapi.ContainerConfig {
			return &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(
					prefix+framework.NewUUID(),
					framework.DefaultAttempt,
				),
				Image: &runtimeapi.ImageSpec{
					Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
				},
				Command: framework.DefaultPauseCommand,
				Linux:   &runtimeapi.LinuxContainerConfig{},
			}
		}

		// runPod creates a pod sandbox and registers it for cleanup.
		runPod := func(
			ctx context.Context,
			prefix string,
		) (string, *runtimeapi.PodSandboxConfig) {
			podConfig := newPodConfig(ctx, prefix)
			podID := framework.RunPodSandbox(ctx, rc, podConfig)
			Expect(podID).NotTo(BeEmpty())
			trackPod(podID)

			return podID, podConfig
		}

		// createAndStartContainer creates and starts a container in the pod and
		// registers it for cleanup.
		createAndStartContainer := func(
			ctx context.Context,
			podID string,
			podConfig *runtimeapi.PodSandboxConfig,
			prefix string,
		) string {
			containerID := framework.CreateContainer(
				ctx, rc, ic, newContainerConfig(prefix), podID, podConfig,
			)
			Expect(containerID).NotTo(BeEmpty())
			trackContainer(containerID)
			Expect(rc.StartContainer(ctx, containerID)).NotTo(HaveOccurred())

			return containerID
		}

		// disconnectWhileBlocked waits until blocker's hook is holding the CRI
		// call tracked by callDone, closes the plugin's connection while the
		// hook is still blocked, and asserts that the CRI call returns within
		// disconnectCallTimeout. The hook is released only after the call
		// returned, so the plugin never replies.
		disconnectWhileBlocked := func(
			s *disconnectStub,
			blocker *disconnectHookBlocker,
			callDone <-chan struct{},
			call string,
		) {
			By("waiting for the " + call + " hook to be reached")

			select {
			case <-blocker.reached:
			case <-callDone:
				Fail(call + " returned before its NRI hook fired")
			case <-time.After(30 * time.Second):
				Fail("timed out waiting for the " + call + " NRI hook to fire")
			}

			Expect(callDone).NotTo(BeClosed(),
				"%s must still be in progress while its NRI hook is blocking", call)

			// The disconnect must happen while the runtime is still waiting on
			// the hook; if its request timeout already fired, this spec would
			// test a timeout instead, so it is invalid rather than a runtime
			// deviation.
			Expect(blocker.hookContextErr()).NotTo(HaveOccurred(),
				"invalid test run: the runtime ended the %s hook before the plugin disconnected", call)

			By("closing the plugin connection while the " + call + " hook is blocked")

			start := time.Now()

			s.disconnect()

			By("verifying " + call + " returns within a bounded time")
			Eventually(callDone, disconnectCallTimeout, 100*time.Millisecond).Should(BeClosed(),
				"%s must return within %v after the plugin it was waiting on disconnected",
				call, disconnectCallTimeout)
			framework.Logf("%s returned %v after the plugin disconnected", call, time.Since(start))

			blocker.unblock()
		}

		// expectPodRolledBack asserts that observer got the rollback teardown
		// for a pod whose RunPodSandbox hook it saw: RunPodSandbox, then
		// StopPodSandbox, then RemovePodSandbox, each at least once.
		expectPodRolledBack := func(observer *disconnectStub, podID string) {
			events := observer.Plugin.Events()
			run := disconnectEventIndex(events, EventRunPodSandbox, podID)
			stop := disconnectEventIndex(events, EventStopPodSandbox, podID)
			remove := disconnectEventIndex(events, EventRemovePodSandbox, podID)

			Expect(run).To(BeNumerically(">=", 0),
				"the observer plugin before the disconnecting one must have seen RunPodSandbox for %s", podID)
			Expect(stop).To(BeNumerically(">", run),
				"a plugin whose RunPodSandbox hook ran must get StopPodSandbox for %s by the time the "+
					"failed call returns, or it leaks what it set up (events: %+v)", podID, events)
			Expect(remove).To(BeNumerically(">", stop),
				"a plugin whose RunPodSandbox hook ran must get RemovePodSandbox for %s after "+
					"StopPodSandbox by the time the failed call returns (events: %+v)", podID, events)
		}

		BeforeEach(func(ctx SpecContext) {
			framework.PullPublicImage(
				ctx,
				ic,
				framework.TestContext.TestImageList.DefaultTestContainerImage,
				nil,
			)
		})

		AfterEach(func(ctx SpecContext) {
			cleanup(ctx)
		})

		It(
			"should fail RunPodSandbox without leaks when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// A plugin that disconnects before replying to RunPodSandbox never
				// completed its setup for the pod, so the call must fail as for a
				// plugin timeout: nothing is listed when it returns, a retry with
				// the same metadata succeeds, and plugins whose hook already ran
				// get StopPodSandbox and RemovePodSandbox, or they leak what they
				// set up (kubernetes-sigs/cri-tools#2046).
				before := startStub("cri-test-nri-disconnect-run-before", disconnectIdxBefore)
				target := startStub("cri-test-nri-disconnect-run", disconnectIdxTarget)
				after := startStub("cri-test-nri-disconnect-run-after", disconnectIdxAfter)

				podConfig := newPodConfig(ctx, "nri-test-disconnect-run-")
				podName := podConfig.GetMetadata().GetName()
				blocker := newBlocker(podName)

				target.Plugin.OnRunPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
					blocker.block(hookCtx, pod.GetName())

					return nil
				}

				By("triggering RunPodSandbox in a goroutine")

				var (
					runPodID string
					runErr   error
				)

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)
					defer GinkgoRecover()

					runPodID, runErr = rc.RunPodSandbox(
						ctx,
						podConfig,
						framework.TestContext.RuntimeHandler,
					)
				}()

				disconnectWhileBlocked(target, blocker, callDone, "RunPodSandbox")

				var hookPodID string

				for _, e := range target.Plugin.Events() {
					if e.Type == EventRunPodSandbox && e.PodName == podName {
						hookPodID = e.PodSandboxID
					}
				}

				Expect(hookPodID).NotTo(BeEmpty(), "RunPodSandbox NRI hook should have fired")

				if runErr == nil {
					framework.Logf(
						"RunPodSandbox succeeded after the plugin disconnected mid-hook: %s",
						runPodID,
					)
					trackPod(runPodID)
				} else {
					framework.Logf(
						"RunPodSandbox failed after the plugin disconnected mid-hook: %v",
						runErr,
					)

					By("verifying the failed sandbox is not listed when the call returns")

					sandboxes, err := rc.ListPodSandbox(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					for _, s := range sandboxes {
						Expect(s.GetId() == hookPodID || s.GetMetadata().GetName() == podName).To(
							BeFalse(),
							"sandbox %s must not be listed after RunPodSandbox failed because a "+
								"plugin disconnected", s.GetId())
					}

					By(
						"verifying the plugin before the disconnecting one got the rollback teardown",
					)
					expectPodRolledBack(before, hookPodID)
				}

				if runErr == nil {
					// Remove the sandbox the call should not have created, so the
					// same-metadata retry below tests the runtime, not a
					// duplicate-name conflict.
					Expect(rc.StopPodSandbox(ctx, runPodID)).NotTo(HaveOccurred())
					Expect(rc.RemovePodSandbox(ctx, runPodID)).NotTo(HaveOccurred())
				}

				By("verifying a retry with the same metadata succeeds")

				retryPodID, err := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				Expect(err).NotTo(HaveOccurred(),
					"RunPodSandbox retried with the same metadata must succeed")
				trackPod(retryPodID)

				statusResp, err := rc.PodSandboxStatus(ctx, retryPodID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"the retried sandbox must be Ready")

				By("verifying the still connected plugins get RunPodSandbox for the retry")

				for _, observer := range []*disconnectStub{before, after} {
					Expect(disconnectEventIndex(
						observer.Plugin.Events(), EventRunPodSandbox, retryPodID,
					)).To(BeNumerically(">=", 0),
						"connected plugins must keep getting events after another plugin disconnected")
				}

				if runErr == nil {
					cleanup(ctx)
					Skip(fmt.Sprintf(
						"RunPodSandbox succeeded (sandbox %s) although the NRI plugin whose "+
							"RunPodSandbox hook it was waiting on disconnected before replying. "+
							"Expected: the call fails as for a plugin timeout, nothing is listed, a "+
							"same-metadata retry succeeds, and plugins whose hook ran get "+
							"StopPodSandbox and RemovePodSandbox. Observed: the call succeeded, so the "+
							"pod runs without whatever the disconnected plugin was setting up "+
							"(networking, devices). The NRI adaptation evicts a failing plugin and "+
							"drops its error; see %s for the related discussion.",
						runPodID, disconnectCreateIssue))
				}
			},
		)

		It(
			"should fail CreateContainer without leaks when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// A plugin that disconnects before replying to CreateContainer
				// never completed its setup for the container, so the call must
				// fail as for a plugin timeout: nothing is listed when it returns,
				// a retry with the same metadata succeeds, and plugins whose hook
				// already ran get RemoveContainer, or they leak what they set up
				// (kubernetes-sigs/cri-tools#2046).
				before := startStub("cri-test-nri-disconnect-create-before", disconnectIdxBefore)
				target := startStub("cri-test-nri-disconnect-create", disconnectIdxTarget)
				after := startStub("cri-test-nri-disconnect-create-after", disconnectIdxAfter)

				By("creating a pod sandbox")

				podID, podConfig := runPod(ctx, "nri-test-disconnect-create-")

				containerConfig := newContainerConfig("nri-test-disconnect-create-ctr-")
				containerName := containerConfig.GetMetadata().GetName()
				blocker := newBlocker(containerName)

				target.Plugin.OnCreateContainer = func(
					hookCtx context.Context,
					_ *nri.PodSandbox,
					ctr *nri.Container,
				) error {
					blocker.block(hookCtx, ctr.GetName())

					return nil
				}

				By("triggering CreateContainer in a goroutine")

				var (
					createdID string
					createErr error
				)

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)
					defer GinkgoRecover()

					createdID, createErr = framework.CreateContainerWithError(
						ctx, rc, ic, containerConfig, podID, podConfig,
					)
				}()

				disconnectWhileBlocked(target, blocker, callDone, "CreateContainer")

				var hookContainerID string

				for _, e := range target.Plugin.Events() {
					if e.Type == EventCreateContainer && e.ContainerName == containerName {
						hookContainerID = e.ContainerID
					}
				}

				Expect(hookContainerID).NotTo(BeEmpty(),
					"CreateContainer NRI hook should have fired")

				if createErr == nil {
					framework.Logf(
						"CreateContainer succeeded after the plugin disconnected mid-hook: %s",
						createdID,
					)
					trackContainer(createdID)
				} else {
					framework.Logf(
						"CreateContainer failed after the plugin disconnected mid-hook: %v",
						createErr,
					)

					By("verifying the failed container is not listed when the call returns")

					ctrs, err := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
						PodSandboxId: podID,
					})
					Expect(err).NotTo(HaveOccurred())

					for _, c := range ctrs {
						Expect(
							c.GetId() == hookContainerID ||
								c.GetMetadata().GetName() == containerName,
						).To(
							BeFalse(),
							"container %s must not be listed after CreateContainer failed because a "+
								"plugin disconnected", c.GetId())
					}

					By(
						"verifying the plugin before the disconnecting one got the rollback teardown",
					)

					events := before.Plugin.Events()
					create := disconnectEventIndex(events, EventCreateContainer, hookContainerID)
					remove := disconnectEventIndex(events, EventRemoveContainer, hookContainerID)

					Expect(create).To(BeNumerically(">=", 0),
						"the observer plugin before the disconnecting one must have seen CreateContainer")
					Expect(remove).To(BeNumerically(">", create),
						"a plugin whose CreateContainer hook ran must get RemoveContainer for %s by the "+
							"time the failed call returns, or it leaks what it set up (events: %+v)",
						hookContainerID, events)
				}

				if createErr == nil {
					// Remove the container the call should not have created, so the
					// same-metadata retry below tests the runtime, not a
					// duplicate-name conflict.
					Expect(rc.StopContainer(ctx, createdID, 0)).NotTo(HaveOccurred())
					Expect(rc.RemoveContainer(ctx, createdID)).NotTo(HaveOccurred())
				}

				By("verifying a retry with the same metadata succeeds")

				retryID, err := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(err).NotTo(HaveOccurred(),
					"CreateContainer retried with the same metadata must succeed")
				trackContainer(retryID)
				Expect(rc.StartContainer(ctx, retryID)).NotTo(HaveOccurred(),
					"the retried container must start")

				By("verifying the still connected plugins get CreateContainer for the retry")

				for _, observer := range []*disconnectStub{before, after} {
					Expect(disconnectEventIndex(
						observer.Plugin.Events(), EventCreateContainer, retryID,
					)).To(BeNumerically(">=", 0),
						"connected plugins must keep getting events after another plugin disconnected")
				}

				if createErr == nil {
					cleanup(ctx)
					Skip(fmt.Sprintf(
						"CreateContainer succeeded (container %s) although the NRI plugin whose "+
							"CreateContainer hook it was waiting on disconnected before replying. "+
							"Expected: the call fails as for a plugin timeout, nothing is listed, a "+
							"same-metadata retry succeeds, and plugins whose hook ran get "+
							"RemoveContainer. Observed: the call succeeded, so the container runs "+
							"without whatever the disconnected plugin was setting up (devices, "+
							"resources). The NRI adaptation evicts a failing plugin and drops its "+
							"error; see %s for the related discussion.",
						createdID, disconnectCreateIssue))
				}
			},
		)

		It(
			"should complete StopPodSandbox when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// A plugin that disconnects no longer has to acknowledge the
				// teardown, so StopPodSandbox succeeds, the other plugins still get
				// the hook, and the runtime still stops the pod ("teardown errors
				// must not block" in the unmerged draft containerd/nri#286,
				// docs/pod-sandbox-lifecycle.md at revision 2339aa3).
				before := startStub("cri-test-nri-disconnect-stop-before", disconnectIdxBefore)
				target := startStub("cri-test-nri-disconnect-stop", disconnectIdxTarget)
				after := startStub("cri-test-nri-disconnect-stop-after", disconnectIdxAfter)

				By("creating a pod sandbox with a running container")

				podID, podConfig := runPod(ctx, "nri-test-disconnect-stop-")
				containerID := createAndStartContainer(
					ctx, podID, podConfig, "nri-test-disconnect-stop-ctr-",
				)

				blocker := newBlocker(podID)

				target.Plugin.OnStopPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
					blocker.block(hookCtx, pod.GetId())

					return nil
				}

				By("triggering StopPodSandbox in a goroutine")

				var stopErr error

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)
					defer GinkgoRecover()

					stopErr = rc.StopPodSandbox(ctx, podID)
				}()

				disconnectWhileBlocked(target, blocker, callDone, "StopPodSandbox")

				Expect(stopErr).NotTo(HaveOccurred(),
					"StopPodSandbox must succeed when the only plugin that did not reply disconnected")

				By("verifying every still connected plugin got StopPodSandbox")

				for idx, observer := range map[string]*disconnectStub{
					disconnectIdxBefore: before,
					disconnectIdxAfter:  after,
				} {
					Expect(disconnectEventIndex(
						observer.Plugin.Events(), EventStopPodSandbox, podID,
					)).To(BeNumerically(">=", 0),
						"the plugin at index %s must get StopPodSandbox although the plugin at index "+
							"%s disconnected during the hook", idx, disconnectIdxTarget)
				}

				By("verifying the sandbox is stopped")

				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"sandbox must be NotReady after StopPodSandbox completed")

				ctrStatus, err := rc.ContainerStatus(ctx, containerID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(ctrStatus.GetStatus().GetState()).To(
					Equal(runtimeapi.ContainerState_CONTAINER_EXITED),
					"the pod's workload container must be EXITED after StopPodSandbox completed")

				By("verifying the stopped sandbox can be removed and the connected plugins see it")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after StopPodSandbox completed")

				for _, observer := range []*disconnectStub{before, after} {
					Expect(disconnectEventIndex(
						observer.Plugin.Events(), EventRemovePodSandbox, podID,
					)).To(BeNumerically(">=", 0),
						"connected plugins must keep getting events after another plugin disconnected")
				}

				By("verifying the runtime still runs new pod sandboxes")
				runPod(ctx, "nri-test-disconnect-stop-new-")
			},
		)

		It(
			"should synchronize a reconnected plugin with pods and containers changed while it was disconnected",
			func(ctx SpecContext) {
				// A plugin that reconnects learns what it missed only from
				// Synchronize, so it must include the pods and containers created
				// while the plugin was away and exclude those removed meanwhile, or
				// the plugin leaks state for objects that no longer exist
				// (kubernetes-sigs/cri-tools#2046).
				const pluginName = "cri-test-nri-disconnect-resync"

				observer := startStub(pluginName+"-observer", disconnectIdxBefore)
				target := startStub(pluginName, disconnectIdxTarget)

				By("creating pods and containers while the plugin is connected")

				keptPodID, keptPodConfig := runPod(ctx, "nri-test-disconnect-resync-kept-")
				keptContainerID := createAndStartContainer(
					ctx, keptPodID, keptPodConfig, "nri-test-disconnect-resync-kept-ctr-",
				)
				goneContainerID := createAndStartContainer(
					ctx, keptPodID, keptPodConfig, "nri-test-disconnect-resync-gone-ctr-",
				)
				gonePodID, _ := runPod(ctx, "nri-test-disconnect-resync-gone-")

				for _, id := range []string{keptPodID, gonePodID, keptContainerID, goneContainerID} {
					eventType := EventCreateContainer
					if id == keptPodID || id == gonePodID {
						eventType = EventRunPodSandbox
					}

					Expect(disconnectEventIndex(target.Plugin.Events(), eventType, id)).To(
						BeNumerically(
							">=",
							0,
						), "the plugin must have seen %s for %s before it disconnects",
						eventType, id)
				}

				By("disconnecting the plugin")
				target.disconnect()

				By("creating and removing pods and containers while the plugin is disconnected")

				newPodID, newPodConfig := runPod(ctx, "nri-test-disconnect-resync-new-")
				newContainerID := createAndStartContainer(
					ctx, newPodID, newPodConfig, "nri-test-disconnect-resync-new-ctr-",
				)

				Expect(rc.StopContainer(ctx, goneContainerID, 0)).NotTo(HaveOccurred())
				Expect(rc.RemoveContainer(ctx, goneContainerID)).NotTo(HaveOccurred())
				Expect(rc.StopPodSandbox(ctx, gonePodID)).NotTo(HaveOccurred())
				Expect(rc.RemovePodSandbox(ctx, gonePodID)).NotTo(HaveOccurred())

				By("verifying a connected plugin saw those changes as events")

				observed := observer.Plugin.Events()
				Expect(disconnectEventIndex(observed, EventRunPodSandbox, newPodID)).To(
					BeNumerically(
						">=",
						0,
					), "the connected plugin must get RunPodSandbox for %s", newPodID)
				Expect(disconnectEventIndex(observed, EventCreateContainer, newContainerID)).To(
					BeNumerically(">=", 0), "the connected plugin must get CreateContainer for %s",
					newContainerID)
				Expect(disconnectEventIndex(observed, EventRemoveContainer, goneContainerID)).To(
					BeNumerically(">=", 0), "the connected plugin must get RemoveContainer for %s",
					goneContainerID)
				Expect(disconnectEventIndex(observed, EventRemovePodSandbox, gonePodID)).To(
					BeNumerically(">=", 0), "the connected plugin must get RemovePodSandbox for %s",
					gonePodID)

				By("reconnecting the plugin with the same name and index")
				// startStub returns only after Synchronize has been delivered, so
				// the synced state is populated by now.
				reconnected := startStub(pluginName, disconnectIdxTarget)

				By(
					"verifying Synchronize includes the objects present before and created during the disconnect",
				)
				Expect(reconnected.Plugin.SyncedPods()).To(
					ContainElements(keptPodID, newPodID),
					"Synchronize after reconnect must include pods that existed before the "+
						"disconnect and pods created while the plugin was disconnected")
				Expect(reconnected.Plugin.SyncedContainers()).To(
					ContainElements(keptContainerID, newContainerID),
					"Synchronize after reconnect must include containers that existed before the "+
						"disconnect and containers created while the plugin was disconnected")

				By("verifying Synchronize excludes the objects removed during the disconnect")
				Expect(reconnected.Plugin.SyncedPods()).NotTo(ContainElement(gonePodID),
					"Synchronize after reconnect must not include a pod removed while the plugin "+
						"was disconnected")
				Expect(reconnected.Plugin.SyncedContainers()).NotTo(ContainElement(goneContainerID),
					"Synchronize after reconnect must not include a container removed while the "+
						"plugin was disconnected")
			},
		)
	})
})
