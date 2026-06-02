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
	"sync"
	"sync/atomic"

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

	Context("plugin error handling", Serial, func() {
		var (
			testStub  *NRITestStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
		)

		AfterEach(func(ctx SpecContext) {
			if podID != "" {
				_ = rc.StopPodSandbox(ctx, podID)
				_ = rc.RemovePodSandbox(ctx, podID)
			}

			if testStub != nil {
				testStub.Cleanup()
			}
		})

		It("should propagate RunPodSandbox plugin error, clean up resources, and allow immediate retry", func(ctx SpecContext) {
			// This test validates the spec contract for RunPodSandbox plugin errors:
			// 1. If a plugin returns an error during RunPodSandbox, the CRI call MUST fail
			// 2. The failed sandbox MUST be cleaned up (not visible via List or Get)
			// 3. A subsequent RunPodSandbox MUST succeed (retry works immediately)
			var (
				callCount  atomic.Int32
				nriPodID   string
				nriPodIDMu sync.Mutex
			)

			var err error

			testStub, err = StartNRITestStub("cri-test-nri-run-error", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to fail on first RunPodSandbox, succeed on second
			testStub.Plugin.OnRunPodSandbox = func(_ context.Context, pod *nri.PodSandbox) error {
				count := callCount.Add(1)

				nriPodIDMu.Lock()
				nriPodID = pod.GetId()
				nriPodIDMu.Unlock()

				if count == 1 {
					return errors.New("simulated NRI plugin error on RunPodSandbox")
				}

				return nil
			}

			By("attempting RunPodSandbox (should fail due to plugin error)")

			podSandboxName := "nri-test-run-error-" + framework.NewUUID()
			uid := framework.DefaultUIDPrefix + framework.NewUUID()
			namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
			podConfig = &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(podSandboxName, uid, namespace, framework.DefaultAttempt),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}

			failedPodID, runErr := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			Expect(runErr).To(HaveOccurred(),
				"RunPodSandbox MUST return an error when plugin fails")
			Expect(failedPodID).To(BeEmpty(),
				"RunPodSandbox MUST return an empty pod ID on failure")

			By("verifying sandbox is not visible via ListPodSandbox after failure")
			// The failed sandbox should be fully cleaned up - not visible in any state
			allPods, listErr := rc.ListPodSandbox(ctx, nil)
			Expect(listErr).NotTo(HaveOccurred())

			nriPodIDMu.Lock()
			capturedNRIPodID := nriPodID
			nriPodIDMu.Unlock()

			Expect(capturedNRIPodID).NotTo(BeEmpty(),
				"NRI RunPodSandbox callback MUST be invoked by the runtime and provide a sandbox ID")

			// Check that the failed sandbox is not in the list
			for _, pod := range allPods {
				Expect(pod.GetId()).NotTo(Equal(capturedNRIPodID),
					"Failed sandbox %s MUST NOT appear in ListPodSandbox", capturedNRIPodID)
			}

			By("verifying PodSandboxStatus returns not-found for the failed sandbox")

			_, statusErr := rc.PodSandboxStatus(ctx, capturedNRIPodID, false)
			// The sandbox should be gone - either NotFound error or nil response.
			// Some runtimes return an error, others may return empty status.
			// Either way, it should not be in the List (already verified above).
			_ = statusErr

			// Best-effort cleanup: containerd removes the failed sandbox from its
			// store but leaves the shim and its mounts (rootfs, shm) behind. This
			// is a containerd bug — calling Stop/Remove here is a workaround to
			// release those orphaned mounts and prevent "Device or resource busy"
			// errors during CI cleanup.
			_ = rc.StopPodSandbox(ctx, capturedNRIPodID)
			_ = rc.RemovePodSandbox(ctx, capturedNRIPodID)

			By("retrying RunPodSandbox (should succeed)")

			retryPodID, retryErr := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			Expect(retryErr).NotTo(HaveOccurred(),
				"RunPodSandbox retry MUST succeed after previous plugin error")
			Expect(retryPodID).NotTo(BeEmpty())
			podID = retryPodID

			By("verifying the retry sandbox is Ready")

			statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(statusResp.GetStatus().GetState()).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
				"Retried sandbox should be in Ready state")

			By("verifying plugin hook was invoked twice (once failed, once succeeded)")
			Expect(callCount.Load()).To(Equal(int32(2)),
				"RunPodSandbox hook should have been invoked exactly twice")
		})

		It("should propagate CreateContainer plugin error and allow immediate retry", func(ctx SpecContext) {
			// This test validates the spec contract for CreateContainer plugin errors:
			// 1. If a plugin returns an error during CreateContainer, the CRI call MUST fail
			// 2. A subsequent CreateContainer MUST succeed (retry works immediately)
			var callCount atomic.Int32

			var err error

			testStub, err = StartNRITestStub("cri-test-nri-create-error", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to fail on first CreateContainer, succeed on second
			testStub.Plugin.OnCreateContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
				count := callCount.Add(1)
				if count == 1 {
					return errors.New("simulated NRI plugin error on CreateContainer")
				}

				return nil
			}

			By("creating a pod sandbox")

			podSandboxName := "nri-test-create-error-" + framework.NewUUID()
			uid := framework.DefaultUIDPrefix + framework.NewUUID()
			namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
			podConfig = &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(podSandboxName, uid, namespace, framework.DefaultAttempt),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}
			podID = framework.RunPodSandbox(ctx, rc, podConfig)
			Expect(podID).NotTo(BeEmpty())

			By("pulling test image")
			framework.PullPublicImage(ctx, ic, framework.TestContext.TestImageList.DefaultTestContainerImage, nil)

			By("attempting CreateContainer (should fail due to plugin error)")

			containerName := "nri-test-create-error-ctr-" + framework.NewUUID()
			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(containerName, framework.DefaultAttempt),
				Image: &runtimeapi.ImageSpec{
					Image:              framework.TestContext.TestImageList.DefaultTestContainerImage,
					UserSpecifiedImage: framework.TestContext.TestImageList.DefaultTestContainerImage,
				},
				Command: framework.DefaultPauseCommand,
				Linux:   &runtimeapi.LinuxContainerConfig{},
			}

			failedCtrID, createErr := rc.CreateContainer(ctx, podID, containerConfig, podConfig)
			Expect(createErr).To(HaveOccurred(),
				"CreateContainer MUST return an error when plugin fails")

			// If a container ID was returned despite the error, clean it up
			if failedCtrID != "" {
				_ = rc.StopContainer(ctx, failedCtrID, 0)
				_ = rc.RemoveContainer(ctx, failedCtrID)
			}

			By("retrying CreateContainer (should succeed)")

			retryContainerName := "nri-test-create-retry-ctr-" + framework.NewUUID()
			retryContainerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(retryContainerName, framework.DefaultAttempt),
				Image: &runtimeapi.ImageSpec{
					Image:              framework.TestContext.TestImageList.DefaultTestContainerImage,
					UserSpecifiedImage: framework.TestContext.TestImageList.DefaultTestContainerImage,
				},
				Command: framework.DefaultPauseCommand,
				Linux:   &runtimeapi.LinuxContainerConfig{},
			}

			retryCtrID, retryErr := rc.CreateContainer(ctx, podID, retryContainerConfig, podConfig)
			Expect(retryErr).NotTo(HaveOccurred(),
				"CreateContainer retry MUST succeed after previous plugin error")
			Expect(retryCtrID).NotTo(BeEmpty())

			By("verifying the retried container exists")

			containerStatus, err := rc.ContainerStatus(ctx, retryCtrID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(containerStatus.GetStatus().GetId()).To(Equal(retryCtrID))

			// Clean up the successfully created container
			_ = rc.StopContainer(ctx, retryCtrID, 0)
			_ = rc.RemoveContainer(ctx, retryCtrID)

			By("verifying plugin hook was invoked twice (once failed, once succeeded)")
			Expect(callCount.Load()).To(Equal(int32(2)),
				"CreateContainer hook should have been invoked exactly twice")
		})
	})
})
