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

// disconnectCallTimeout bounds how long a CRI call that was held by a blocked
// NRI hook may take to return once the plugin connection is closed.
const disconnectCallTimeout = 30 * time.Second

// disconnectHookBlocker holds the first invocation of an NRI hook until the
// test releases it (or the hook context is cancelled), so the test can close
// the plugin connection while the runtime is waiting on the hook.
type disconnectHookBlocker struct {
	reached     chan struct{}
	release     chan struct{}
	blockOnce   sync.Once
	releaseOnce sync.Once
}

func newDisconnectHookBlocker() *disconnectHookBlocker {
	return &disconnectHookBlocker{
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// block is called from inside an NRI hook. Only the first invocation blocks;
// later ones (e.g. cleanup-triggered hooks) return immediately.
func (b *disconnectHookBlocker) block(hookCtx context.Context) {
	first := false

	b.blockOnce.Do(func() { first = true })

	if !first {
		return
	}

	close(b.reached)

	select {
	case <-b.release:
	case <-hookCtx.Done():
	}
}

// unblock releases the blocked hook. It is idempotent.
func (b *disconnectHookBlocker) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
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
			stubs      []*NRITestStub
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

		// startStub connects an NRI test stub and registers it for cleanup.
		startStub := func(name string) *NRITestStub {
			stub, err := StartNRITestStub(name, "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub %s", name)

			trackMu.Lock()

			stubs = append(stubs, stub)
			trackMu.Unlock()

			return stub
		}

		// newBlocker creates a hook blocker that AfterEach always releases.
		newBlocker := func() *disconnectHookBlocker {
			b := newDisconnectHookBlocker()

			trackMu.Lock()

			blockers = append(blockers, b)
			trackMu.Unlock()

			return b
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
		// call tracked by callDone, closes the plugin connection, and asserts
		// that the CRI call returns within disconnectCallTimeout.
		disconnectWhileBlocked := func(
			stub *NRITestStub,
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

			By("closing the plugin connection while the " + call + " hook is blocked")
			stub.Stop()
			// The connection is closed by now; let the stub's handler goroutine
			// finish so it does not linger past the spec.
			blocker.unblock()

			By("verifying " + call + " returns within a bounded time")
			Eventually(callDone, disconnectCallTimeout, 100*time.Millisecond).Should(BeClosed(),
				"%s MUST return (with success or error) within %v after the blocking NRI plugin "+
					"disconnected", call, disconnectCallTimeout)
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
			trackMu.Lock()
			specStubs, specBlockers := stubs, blockers
			podIDs, containerIDs := pods, containers
			stubs, blockers, pods, containers = nil, nil, nil, nil
			trackMu.Unlock()

			// Also clean up anything the runtime announced through a hook, in
			// case the spec failed before the CRI call returned its ID.
			for _, stub := range specStubs {
				for _, e := range stub.Plugin.Events() {
					if e.Type == EventRunPodSandbox {
						podIDs = append(podIDs, e.PodSandboxID)
					}

					if e.Type == EventCreateContainer {
						containerIDs = append(containerIDs, e.ContainerID)
					}
				}
			}

			// Stop every stub (Stop is safe on an already stopped stub) and
			// release every hook so no CRI call stays blocked on the plugin.
			for _, stub := range specStubs {
				stub.Cleanup()
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
					framework.Logf("AfterEach: StopContainer(%s) failed: %v", id, err)
				}

				if err := rc.RemoveContainer(ctx, id); err != nil {
					framework.Logf("AfterEach: RemoveContainer(%s) failed: %v", id, err)
				}
			}

			for _, id := range podIDs {
				if id == "" || seen[id] {
					continue
				}

				seen[id] = true

				if err := rc.StopPodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", id, err)
				}

				if err := rc.RemovePodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", id, err)
				}
			}
		})

		It(
			"should complete RunPodSandbox without leaks when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// Contract: if a plugin disconnects while the runtime is waiting on
				// its RunPodSandbox hook, the CRI RunPodSandbox call MUST NOT hang.
				// It may succeed or fail, but a failed call MUST NOT leave the
				// sandbox behind, and the runtime MUST keep serving new requests.
				stub := startStub("cri-test-nri-disconnect-run")
				blocker := newBlocker()

				stub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
					blocker.block(hookCtx)

					return nil
				}

				By("triggering RunPodSandbox in a goroutine")

				podConfig := newPodConfig(ctx, "nri-test-disconnect-run-")
				podName := podConfig.GetMetadata().GetName()

				var (
					runPodID string
					runErr   error
				)

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)

					runPodID, runErr = rc.RunPodSandbox(
						ctx,
						podConfig,
						framework.TestContext.RuntimeHandler,
					)
				}()

				disconnectWhileBlocked(stub, blocker, callDone, "RunPodSandbox")

				hookPodID := stub.Plugin.LastRunPodSandboxID()
				Expect(hookPodID).NotTo(BeEmpty(), "RunPodSandbox NRI hook should have fired")

				if runErr == nil {
					By("verifying the sandbox created across the disconnect is Ready")
					trackPod(runPodID)

					statusResp, err := rc.PodSandboxStatus(ctx, runPodID, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(statusResp.GetStatus().GetState()).To(
						Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"a sandbox returned by a successful RunPodSandbox MUST be Ready")
				} else {
					framework.Logf("RunPodSandbox failed after plugin disconnect: %v", runErr)

					By("verifying the failed sandbox is not left behind")

					sandboxes, err := rc.ListPodSandbox(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					for _, s := range sandboxes {
						Expect(s.GetId() == hookPodID || s.GetMetadata().GetName() == podName).To(
							BeFalse(),
							"sandbox %s MUST be cleaned up after RunPodSandbox failed due to a "+
								"plugin disconnect", s.GetId())
					}
				}

				By("verifying the runtime still runs new pod sandboxes")

				newPodID, _ := runPod(ctx, "nri-test-disconnect-run-after-")

				statusResp, err := rc.PodSandboxStatus(ctx, newPodID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"new sandbox should be Ready after the plugin disconnected")
			},
		)

		It(
			"should complete CreateContainer without leaks when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// Contract: if a plugin disconnects while the runtime is waiting on
				// its CreateContainer hook, the CRI CreateContainer call MUST NOT
				// hang. It may succeed or fail, but a failed call MUST NOT leave the
				// container behind, and the runtime MUST keep serving new requests.
				stub := startStub("cri-test-nri-disconnect-create")
				blocker := newBlocker()

				stub.Plugin.OnCreateContainer = func(
					hookCtx context.Context,
					_ *nri.PodSandbox,
					_ *nri.Container,
				) error {
					blocker.block(hookCtx)

					return nil
				}

				By("creating a pod sandbox")

				podID, podConfig := runPod(ctx, "nri-test-disconnect-create-")

				By("triggering CreateContainer in a goroutine")

				containerConfig := newContainerConfig("nri-test-disconnect-create-ctr-")
				containerName := containerConfig.GetMetadata().GetName()

				var (
					createdID string
					createErr error
				)

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)

					createdID, createErr = framework.CreateContainerWithError(
						ctx, rc, ic, containerConfig, podID, podConfig,
					)
				}()

				disconnectWhileBlocked(stub, blocker, callDone, "CreateContainer")

				var hookContainerID string

				for _, e := range stub.Plugin.Events() {
					if e.Type == EventCreateContainer {
						hookContainerID = e.ContainerID
					}
				}

				Expect(hookContainerID).NotTo(BeEmpty(),
					"CreateContainer NRI hook should have fired")

				if createErr == nil {
					By("verifying the container created across the disconnect is usable")
					trackContainer(createdID)
					Expect(rc.StartContainer(ctx, createdID)).NotTo(HaveOccurred(),
						"a container returned by a successful CreateContainer MUST be startable")
				} else {
					framework.Logf("CreateContainer failed after plugin disconnect: %v", createErr)

					By("verifying the failed container is not left behind")

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
							"container %s MUST be cleaned up after CreateContainer failed due to a "+
								"plugin disconnect", c.GetId())
					}
				}

				By("verifying the runtime still creates and starts new containers in the pod")
				createAndStartContainer(ctx, podID, podConfig, "nri-test-disconnect-create-after-")
			},
		)

		It(
			"should complete StopPodSandbox when the plugin disconnects during the hook",
			func(ctx SpecContext) {
				// Contract: if a plugin disconnects while the runtime is waiting on
				// its StopPodSandbox hook, the CRI StopPodSandbox call MUST NOT hang
				// and the sandbox MUST end up stopped, so it can be removed. The
				// runtime MUST keep serving new requests.
				stub := startStub("cri-test-nri-disconnect-stop")
				blocker := newBlocker()

				stub.Plugin.OnStopPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
					blocker.block(hookCtx)

					return nil
				}

				By("creating a pod sandbox with a running container")

				podID, podConfig := runPod(ctx, "nri-test-disconnect-stop-")
				createAndStartContainer(ctx, podID, podConfig, "nri-test-disconnect-stop-ctr-")

				By("triggering StopPodSandbox in a goroutine")

				var stopErr error

				callDone := make(chan struct{})

				go func() {
					defer close(callDone)

					stopErr = rc.StopPodSandbox(ctx, podID)
				}()

				disconnectWhileBlocked(stub, blocker, callDone, "StopPodSandbox")

				Expect(stopErr).NotTo(HaveOccurred(),
					"StopPodSandbox MUST complete successfully after the blocking NRI plugin disconnected")

				By("verifying the sandbox is stopped")

				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"sandbox MUST be NotReady after StopPodSandbox completed")

				By("verifying the stopped sandbox can be removed")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after StopPodSandbox completed")

				By("verifying the runtime still runs new pod sandboxes")
				runPod(ctx, "nri-test-disconnect-stop-after-")
			},
		)

		It(
			"should synchronize a reconnected plugin with pods and containers changed while it was disconnected",
			func(ctx SpecContext) {
				// Contract: when a plugin reconnects after a disconnect, the runtime
				// MUST call Synchronize with the current pods and containers,
				// including those created while the plugin was disconnected, and
				// excluding those removed in the meantime.
				const pluginName = "cri-test-nri-disconnect-resync"

				stub := startStub(pluginName)

				By("creating a pod and container while the plugin is connected")

				keptPodID, keptPodConfig := runPod(ctx, "nri-test-disconnect-resync-kept-")
				keptContainerID := createAndStartContainer(
					ctx, keptPodID, keptPodConfig, "nri-test-disconnect-resync-kept-ctr-",
				)

				goneConfig := newPodConfig(ctx, "nri-test-disconnect-resync-gone-")
				gonePodID := framework.RunPodSandbox(ctx, rc, goneConfig)
				Expect(gonePodID).NotTo(BeEmpty())
				trackPod(gonePodID)

				By("disconnecting the plugin")
				stub.Stop()

				By("creating a pod and container while the plugin is disconnected")

				newPodID, newPodConfig := runPod(ctx, "nri-test-disconnect-resync-new-")
				newContainerID := createAndStartContainer(
					ctx, newPodID, newPodConfig, "nri-test-disconnect-resync-new-ctr-",
				)

				By("removing a pod while the plugin is disconnected")
				Expect(rc.StopPodSandbox(ctx, gonePodID)).NotTo(HaveOccurred())
				Expect(rc.RemovePodSandbox(ctx, gonePodID)).NotTo(HaveOccurred())

				By("verifying the disconnected plugin received no events for the new pod")
				Expect(FilterEventsByPodID(stub.Plugin.Events(), newPodID)).To(BeEmpty(),
					"a disconnected plugin must not receive events")

				By("reconnecting the plugin")
				// StartNRITestStub returns only after Synchronize has been
				// delivered, so the synced state is populated by now.
				reconnected := startStub(pluginName)

				By(
					"verifying Synchronize includes pods present before and created during the disconnect",
				)
				Expect(reconnected.Plugin.SyncedPods()).To(
					ContainElements(keptPodID, newPodID),
					"Synchronize after reconnect MUST include pods that existed before the "+
						"disconnect and pods created while the plugin was disconnected")

				By(
					"verifying Synchronize includes containers present before and created during the disconnect",
				)
				Expect(reconnected.Plugin.SyncedContainers()).To(
					ContainElements(keptContainerID, newContainerID),
					"Synchronize after reconnect MUST include containers that existed before the "+
						"disconnect and containers created while the plugin was disconnected")

				By("verifying Synchronize excludes the pod removed during the disconnect")
				Expect(reconnected.Plugin.SyncedPods()).NotTo(ContainElement(gonePodID),
					"Synchronize after reconnect MUST NOT include a pod removed while the plugin "+
						"was disconnected")
			},
		)
	})
})
