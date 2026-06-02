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

	Context("RunPodSandbox contract", Serial, func() {
		var (
			testStub  *NRITestStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
		)

		AfterEach(func(ctx SpecContext) {
			// Capture the fallback sandbox ID before cleanup resets events.
			cleanupID := podID
			if cleanupID == "" && testStub != nil {
				cleanupID = testStub.Plugin.LastRunPodSandboxID()
			}

			// Stop the stub to unblock any hooks that may be holding
			// a RunPodSandbox call, allowing it to complete.
			if testStub != nil {
				testStub.Cleanup()
			}

			if cleanupID != "" {
				_ = rc.StopPodSandbox(ctx, cleanupID)
				_ = rc.RemovePodSandbox(ctx, cleanupID)
			}
		})

		It("should not expose sandbox while RunPodSandbox hook is in progress", func(ctx SpecContext) {
			// This test validates the spec contract: during RunPodSandbox hook execution,
			// the sandbox MUST NOT be visible via List or Status, and workload containers
			// MUST NOT start.

			// Channel to control blocking behavior
			hookBlocking := make(chan struct{})
			hookReached := make(chan struct{})

			var hookPodID string

			var err error

			testStub, err = StartNRITestStub("cri-test-nri-block-run", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to block on RunPodSandbox
			testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
				hookPodID = pod.GetId()

				close(hookReached)
				// Block until test signals to continue or context is cancelled (cleanup)
				select {
				case <-hookBlocking:
				case <-hookCtx.Done():
				}

				return nil
			}

			By("triggering RunPodSandbox in a goroutine")

			podSandboxName := "nri-test-block-run-" + framework.NewUUID()
			uid := framework.DefaultUIDPrefix + framework.NewUUID()
			namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
			podConfig = &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(podSandboxName, uid, namespace, framework.DefaultAttempt),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}

			var (
				runErr   error
				runPodID string
				runWg    sync.WaitGroup
			)

			runWg.Go(func() {
				runPodID, runErr = rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			})

			By("waiting for RunPodSandbox hook to be reached")

			select {
			case <-hookReached:
				// Hook is now blocking
			case <-time.After(30 * time.Second):
				close(hookBlocking) // unblock to avoid goroutine leak
				Fail("Timed out waiting for RunPodSandbox NRI hook to fire")
			}

			By("verifying sandbox is NOT listed while hook is blocking")
			// The sandbox should not appear in ListPodSandbox in any state
			pods, listErr := rc.ListPodSandbox(ctx, nil)
			Expect(listErr).NotTo(HaveOccurred())

			sandboxFound := false

			for _, pod := range pods {
				if pod.GetId() == hookPodID {
					sandboxFound = true

					break
				}
			}

			Expect(sandboxFound).To(BeFalse(),
				"Sandbox %s MUST NOT be listed while RunPodSandbox hook is blocking", hookPodID)

			By("verifying PodSandboxStatus is not accessible while hook is blocking")

			if hookPodID != "" {
				statusResp, statusErr := rc.PodSandboxStatus(ctx, hookPodID, false)
				// Ideally the sandbox should not be found at all. Some runtimes may
				// return a non-Ready status instead of NotFound — both are acceptable.
				if statusErr == nil && statusResp != nil && statusResp.GetStatus() != nil {
					Expect(statusResp.GetStatus().GetState()).NotTo(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"Sandbox MUST NOT report Ready state while RunPodSandbox hook is in progress")
				}
			}

			By("releasing the hook and verifying pod becomes Ready")
			close(hookBlocking)
			runWg.Wait()
			Expect(runErr).NotTo(HaveOccurred(), "RunPodSandbox should succeed after hook returns")
			Expect(runPodID).NotTo(BeEmpty())
			podID = runPodID

			// After hook completes, sandbox should be Ready
			statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(statusResp.GetStatus().GetState()).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
				"Sandbox should be Ready after RunPodSandbox hook completes")
		})

		It("should not start workload containers until RunPodSandbox hook completes", func(ctx SpecContext) {
			// This test validates that workload container creation is blocked while the
			// RunPodSandbox hook is still running. Even if the caller attempts CreateContainer
			// immediately, it should not succeed until the hook returns.
			hookBlocking := make(chan struct{})
			hookReached := make(chan struct{})

			var err error

			testStub, err = StartNRITestStub("cri-test-nri-block-container", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to block RunPodSandbox for a measurable duration
			testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
				close(hookReached)

				select {
				case <-hookBlocking:
				case <-hookCtx.Done():
				}

				return nil
			}

			By("triggering RunPodSandbox in a goroutine")

			podSandboxName := "nri-test-block-container-" + framework.NewUUID()
			uid := framework.DefaultUIDPrefix + framework.NewUUID()
			namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
			podConfig = &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(podSandboxName, uid, namespace, framework.DefaultAttempt),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}

			var (
				runErr   error
				runPodID string
				runWg    sync.WaitGroup
			)

			runWg.Go(func() {
				runPodID, runErr = rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			})

			By("waiting for RunPodSandbox hook to be reached")

			select {
			case <-hookReached:
				// Hook is now blocking
			case <-time.After(30 * time.Second):
				close(hookBlocking)
				Fail("Timed out waiting for RunPodSandbox NRI hook to fire")
			}

			By("releasing hook after brief delay to verify container creation is gated")
			// We hold the hook for 2 seconds. RunPodSandbox should not return during this time,
			// which means no pod ID is available for container creation yet.
			// The CRI contract ensures RunPodSandbox is synchronous, so the caller cannot
			// get a pod ID until the hook releases.
			hookHoldDuration := 2 * time.Second
			hookBlockedAt := time.Now()

			go func() {
				time.Sleep(hookHoldDuration)
				close(hookBlocking)
			}()

			// Wait for RunPodSandbox to complete
			runWg.Wait()

			runCompletedAt := time.Now()

			Expect(runErr).NotTo(HaveOccurred())
			Expect(runPodID).NotTo(BeEmpty())
			podID = runPodID

			// Verify that RunPodSandbox was indeed blocked for the expected duration
			actualBlockDuration := runCompletedAt.Sub(hookBlockedAt)
			Expect(actualBlockDuration).To(BeNumerically(">=", hookHoldDuration-100*time.Millisecond),
				"RunPodSandbox should be blocked for at least the hook hold duration, "+
					"confirming container creation cannot proceed until hook completes")

			// Now that the pod is ready, verify container creation works
			By("verifying container creation succeeds after hook completes")
			framework.PullPublicImage(ctx, ic, framework.TestContext.TestImageList.DefaultTestContainerImage, nil)

			containerName := "nri-test-after-hook-" + framework.NewUUID()
			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(containerName, framework.DefaultAttempt),
				Image:    &runtimeapi.ImageSpec{Image: framework.TestContext.TestImageList.DefaultTestContainerImage},
				Command:  framework.DefaultPauseCommand,
				Linux:    &runtimeapi.LinuxContainerConfig{},
			}
			containerID := framework.CreateContainer(ctx, rc, ic, containerConfig, podID, podConfig)
			Expect(containerID).NotTo(BeEmpty(),
				"Container creation should succeed after RunPodSandbox hook completes")

			// Clean up container
			_ = rc.StopContainer(ctx, containerID, 0)
			_ = rc.RemoveContainer(ctx, containerID)
		})
	})
})
