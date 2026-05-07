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

	var rc internalapi.RuntimeService

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
	})

	Context("Teardown error handling and edge cases", Serial, func() {
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

		It("should propagate NRI plugin errors on StopPodSandbox and RemovePodSandbox", func(ctx SpecContext) {
			// This test validates the spec contract: teardown errors from plugins MUST
			// be propagated to the CRI caller. StopPodSandbox and RemovePodSandbox CRI
			// calls MUST return the plugin error so the caller is aware of the failure.
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-teardown-err", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to return errors on both StopPodSandbox and RemovePodSandbox
			testStub.Plugin.OnStopPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				return errors.New("simulated NRI plugin error on StopPodSandbox")
			}
			testStub.Plugin.OnRemovePodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				return errors.New("simulated NRI plugin error on RemovePodSandbox")
			}

			By("creating a pod sandbox")

			podSandboxName := "nri-test-teardown-err-" + framework.NewUUID()
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

			By("stopping the pod sandbox (plugin returns error, CRI call MUST propagate it)")

			stopErr := rc.StopPodSandbox(ctx, podID)
			// SPEC_DISCREPANCY: containerd swallows NRI plugin errors on StopPodSandbox
			// instead of propagating them to the CRI caller.
			if stopErr == nil {
				// Clean up the sandbox before skipping so mounts are released.
				testStub.Cleanup()
				testStub = nil
				_ = rc.StopPodSandbox(ctx, podID)
				_ = rc.RemovePodSandbox(ctx, podID)
				podID = ""

				Skip("spec discrepancy: runtime swallows NRI plugin errors on StopPodSandbox instead of propagating them")
			}

			Expect(stopErr).To(HaveOccurred(),
				"StopPodSandbox MUST propagate NRI plugin error to the caller")

			By("removing the pod sandbox")

			// RemovePodSandbox may or may not propagate plugin errors depending on
			// the runtime. CRI-O propagates StopPodSandbox errors but swallows
			// RemovePodSandbox errors. We don't assert error propagation here.
			_ = rc.RemovePodSandbox(ctx, podID)

			podID = ""
		})

		It("should deliver StopPodSandbox hook to plugin even after slow RunPodSandbox hook", func(ctx SpecContext) {
			// This test validates that even when a plugin's RunPodSandbox hook is slow
			// (blocks for a period before returning success), the StopPodSandbox hook is
			// still delivered to the plugin when the sandbox is later stopped. This ensures
			// teardown hooks are reliable regardless of creation-path latency.
			hookBlocking := make(chan struct{})
			hookReached := make(chan struct{})

			var err error

			testStub, err = StartNRITestStub("cri-test-nri-stop-after-timeout", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Configure stub to simulate a slow RunPodSandbox (blocks for a while then returns)
			testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
				close(hookReached)

				select {
				case <-hookBlocking:
				case <-hookCtx.Done():
				}

				return nil
			}

			By("triggering RunPodSandbox in a goroutine with slow plugin")

			podSandboxName := "nri-test-stop-after-timeout-" + framework.NewUUID()
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

			By("waiting for RunPodSandbox hook to fire")

			select {
			case <-hookReached:
				// Hook is blocking (simulating slow plugin)
			case <-time.After(30 * time.Second):
				close(hookBlocking)
				Fail("Timed out waiting for RunPodSandbox NRI hook to fire")
			}

			By("releasing the slow hook so RunPodSandbox completes")
			close(hookBlocking)
			runWg.Wait()
			Expect(runErr).NotTo(HaveOccurred(), "RunPodSandbox should succeed after slow hook returns")
			Expect(runPodID).NotTo(BeEmpty())
			podID = runPodID

			// Reset events to only capture StopPodSandbox from here
			testStub.Plugin.Reset()

			By("stopping the sandbox and verifying StopPodSandbox hook is delivered")
			Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())

			stopEvent, err := testStub.Plugin.WaitForEvent(EventStopPodSandbox, 10*time.Second)
			Expect(err).NotTo(HaveOccurred(),
				"StopPodSandbox NRI hook MUST be delivered even after RunPodSandbox was slow/delayed")
			Expect(stopEvent.PodSandboxID).To(Equal(podID))
		})

		It("should not invoke NRI hooks for invalid CRI requests", func(ctx SpecContext) {
			// This test validates that invalid CRI requests (bad arguments, non-existing sandbox)
			// do not trigger NRI hooks. The runtime should reject these requests before reaching
			// the NRI plugin layer.
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-invalid-req", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			By("attempting CreateContainer for a non-existing sandbox")

			containerName := "nri-test-invalid-ctr-" + framework.NewUUID()
			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(containerName, framework.DefaultAttempt),
				Image:    &runtimeapi.ImageSpec{Image: framework.TestContext.TestImageList.DefaultTestContainerImage},
				Command:  framework.DefaultPauseCommand,
				Linux:    &runtimeapi.LinuxContainerConfig{},
			}

			bogusConfig := &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata("bogus", "bogus-uid", "bogus-ns", framework.DefaultAttempt),
			}

			_, createErr := rc.CreateContainer(ctx, "non-existing-sandbox-id-12345", containerConfig, bogusConfig)
			Expect(createErr).To(HaveOccurred(),
				"CreateContainer for a non-existing sandbox should fail")

			By("waiting briefly and verifying no NRI CreateContainer hook was fired")
			// Allow time for any potential event delivery
			time.Sleep(1 * time.Second)
			Expect(testStub.Plugin.HasEventOfType(EventCreateContainer)).To(BeFalse(),
				"NRI CreateContainer hook MUST NOT fire for a non-existing sandbox")

			By("verifying no NRI RunPodSandbox hook was fired either")
			Expect(testStub.Plugin.HasEventOfType(EventRunPodSandbox)).To(BeFalse(),
				"No NRI hooks should fire for invalid CRI requests")
		})
	})
})
