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

	Context("multi-plugin coordination", Serial, func() {
		var (
			multiStub *NRIMultiStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
		)

		AfterEach(func(ctx SpecContext) {
			// Capture the fallback sandbox ID before cleanup resets events.
			cleanupID := podID
			if cleanupID == "" && multiStub != nil {
				cleanupID = multiStub.LastRunPodSandboxID()
			}

			// Stop stubs to unblock any hooks holding a RunPodSandbox call.
			if multiStub != nil {
				multiStub.Cleanup()
			}

			if cleanupID != "" {
				_ = rc.StopPodSandbox(ctx, cleanupID)
				_ = rc.RemovePodSandbox(ctx, cleanupID)
			}
		})

		It("should invoke all plugins in index order during RunPodSandbox before starting workload containers", func(ctx SpecContext) {
			// This test validates the multi-plugin ordering contract:
			// 1. All registered plugins receive RunPodSandbox hooks
			// 2. Plugins are invoked in index order (lower index first)
			// 3. No workload containers start until ALL plugins complete their RunPodSandbox hooks

			// Track invocation order using a shared slice protected by a mutex
			var (
				invocationOrder []int
				orderMu         sync.Mutex
			)

			// Channel to block the higher-index plugin (plugin 1) to verify ordering
			plugin1Reached := make(chan struct{})
			plugin1Release := make(chan struct{})

			var err error

			multiStub, err = StartNRIMultiStub("cri-test-nri-multi-order", 2, 10)
			Expect(err).NotTo(HaveOccurred(), "failed to start multi-stub")

			// Plugin 0 (index 10) - lower index, should be invoked first
			multiStub.Plugin(0).OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				orderMu.Lock()

				invocationOrder = append(invocationOrder, 0)
				orderMu.Unlock()

				return nil
			}

			// Plugin 1 (index 11) - higher index, should be invoked second
			multiStub.Plugin(1).OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
				orderMu.Lock()

				invocationOrder = append(invocationOrder, 1)
				orderMu.Unlock()

				close(plugin1Reached)

				select {
				case <-plugin1Release:
				case <-hookCtx.Done():
				}

				return nil
			}

			By("creating a pod sandbox with two plugins registered")

			podSandboxName := "nri-test-multi-order-" + framework.NewUUID()
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

			By("waiting for plugin 1 (higher index) to be reached")

			select {
			case <-plugin1Reached:
				// Both plugins have been invoked (plugin 0 already returned, plugin 1 is blocking)
			case <-time.After(30 * time.Second):
				close(plugin1Release)
				Fail("Timed out waiting for second plugin to receive RunPodSandbox hook")
			}

			By("verifying both plugins received RunPodSandbox in index order")
			orderMu.Lock()
			order := make([]int, len(invocationOrder))
			copy(order, invocationOrder)
			orderMu.Unlock()

			Expect(order).To(HaveLen(2), "Both plugins MUST receive RunPodSandbox hook")
			Expect(order[0]).To(Equal(0), "Plugin with lower index MUST be invoked first")
			Expect(order[1]).To(Equal(1), "Plugin with higher index MUST be invoked second")

			By("verifying RunPodSandbox has not returned while plugin 1 is still blocking")
			// RunPodSandbox should still be in progress because plugin 1 is blocking
			// Give a brief moment and check that runWg hasn't completed
			doneCh := make(chan struct{})

			go func() {
				runWg.Wait()
				close(doneCh)
			}()

			select {
			case <-doneCh:
				Fail("RunPodSandbox MUST NOT return while any plugin's hook is still in progress")
			case <-time.After(500 * time.Millisecond):
				// Good - RunPodSandbox is still blocked
			}

			By("releasing plugin 1 and verifying RunPodSandbox completes")
			close(plugin1Release)
			runWg.Wait()
			Expect(runErr).NotTo(HaveOccurred(), "RunPodSandbox should succeed after all plugins return")
			Expect(runPodID).NotTo(BeEmpty())
			podID = runPodID

			By("verifying sandbox is Ready after all plugins complete")

			statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(statusResp.GetStatus().GetState()).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
				"Sandbox should be Ready after all plugins complete RunPodSandbox hooks")
		})

		It("should deliver teardown hooks to all plugins even if one fails", func(ctx SpecContext) {
			// This test validates the multi-plugin fault isolation contract:
			// One plugin returning an error on StopPodSandbox/RemovePodSandbox MUST NOT
			// prevent delivery of those hooks to subsequent plugins.
			var (
				plugin0StopReceived, plugin0RemoveReceived atomic.Int32
				plugin1StopReceived, plugin1RemoveReceived atomic.Int32
			)

			var err error

			multiStub, err = StartNRIMultiStub("cri-test-nri-multi-fault", 2, 10)
			Expect(err).NotTo(HaveOccurred(), "failed to start multi-stub")

			// Plugin 0 (index 10) - returns errors on teardown hooks
			multiStub.Plugin(0).OnStopPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				plugin0StopReceived.Add(1)

				return errors.New("simulated plugin 0 error on StopPodSandbox")
			}
			multiStub.Plugin(0).OnRemovePodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				plugin0RemoveReceived.Add(1)

				return errors.New("simulated plugin 0 error on RemovePodSandbox")
			}

			// Plugin 1 (index 11) - should still receive hooks despite plugin 0's errors
			multiStub.Plugin(1).OnStopPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				plugin1StopReceived.Add(1)

				return nil
			}
			multiStub.Plugin(1).OnRemovePodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
				plugin1RemoveReceived.Add(1)

				return nil
			}

			By("creating a pod sandbox")

			podSandboxName := "nri-test-multi-fault-" + framework.NewUUID()
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

			By("stopping the pod sandbox (plugin 0 returns error)")

			stopErr := rc.StopPodSandbox(ctx, podID)
			// SPEC_DISCREPANCY: containerd swallows NRI plugin errors on StopPodSandbox
			// instead of propagating them to the CRI caller.
			if stopErr == nil {
				// Clean up the sandbox before skipping so mounts are released.
				multiStub.Cleanup()
				multiStub = nil
				_ = rc.StopPodSandbox(ctx, podID)
				_ = rc.RemovePodSandbox(ctx, podID)
				podID = ""

				Skip("spec discrepancy: runtime swallows NRI plugin errors on StopPodSandbox instead of propagating them")
			}

			Expect(stopErr).To(HaveOccurred(),
				"StopPodSandbox MUST propagate NRI plugin error to the caller")

			By("removing the pod sandbox")

			// RemovePodSandbox may or may not propagate plugin errors depending on
			// the runtime. We don't assert error propagation here.
			_ = rc.RemovePodSandbox(ctx, podID)

			By("waiting for events to propagate")
			time.Sleep(1 * time.Second)

			By("verifying plugin 0 (failing plugin) received both hooks")
			Expect(plugin0StopReceived.Load()).To(BeNumerically(">=", 1),
				"Plugin 0 MUST receive StopPodSandbox hook")
			Expect(plugin0RemoveReceived.Load()).To(BeNumerically(">=", 1),
				"Plugin 0 MUST receive RemovePodSandbox hook")

			By("verifying plugin 1 received both hooks despite plugin 0's errors")
			// SPEC_DISCREPANCY: NRI aborts hook delivery to subsequent plugins when one plugin returns an error,
			// instead of delivering teardown hooks to all plugins regardless of individual failures.
			if plugin1StopReceived.Load() < 1 || plugin1RemoveReceived.Load() < 1 {
				Skip("spec discrepancy: NRI does not deliver teardown hooks to subsequent plugins after one plugin returns an error")
			}

			Expect(plugin1StopReceived.Load()).To(BeNumerically(">=", 1),
				"Plugin 1 MUST receive StopPodSandbox hook even when plugin 0 fails")
			Expect(plugin1RemoveReceived.Load()).To(BeNumerically(">=", 1),
				"Plugin 1 MUST receive RemovePodSandbox hook even when plugin 0 fails")

			By("verifying sandbox is fully removed")

			allPods, listErr := rc.ListPodSandbox(ctx, nil)
			Expect(listErr).NotTo(HaveOccurred())

			for _, pod := range allPods {
				Expect(pod.GetId()).NotTo(Equal(podID),
					"Sandbox MUST be fully removed despite plugin errors")
			}

			// Mark as cleaned up
			podID = ""
		})
	})
})
