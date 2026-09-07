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
	"slices"
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
		rc, ic = nriTestClients(f)
	})

	Context("RunPodSandbox contract", Serial, func() {
		var (
			testStub  *NRITestStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
			// joinInFlightRun is set by specs that leave a RunPodSandbox
			// blocked in a goroutine. AfterEach calls it to release the hook and
			// join the goroutine, so the cleanup below never issues its
			// stop/remove calls concurrently with a sandbox that is still being
			// created.
			joinInFlightRun func()
		)

		AfterEach(func(ctx SpecContext) {
			// Release and join any RunPodSandbox left in flight by a spec that
			// aborted between the hook handshake and its own Wait(). This must
			// happen before the IDs are collected below so the sandbox is fully
			// created by the time it is stopped and removed.
			if joinInFlightRun != nil {
				joinInFlightRun()
			}

			// Capture the sandbox IDs to clean up before the stub is stopped
			// and its recorded events are dropped. A spec that fails before
			// assigning podID still leaks the sandbox its goroutine created, so
			// the stub's last observed RunPodSandbox ID is always collected as
			// well rather than only as a fallback. The lookup is scoped to this
			// suite's pod name prefix so cleanup can never stop and remove a
			// sandbox that some other actor on the node created meanwhile.
			cleanupIDs := []string{}
			if podID != "" {
				cleanupIDs = append(cleanupIDs, podID)
			}

			if testStub != nil {
				lastID := testStub.Plugin.LastRunPodSandboxID(nriTestPodNamePrefix)
				if lastID != "" && !slices.Contains(cleanupIDs, lastID) {
					cleanupIDs = append(cleanupIDs, lastID)
				}
			}

			// Stop the stub to unblock any hooks that may be holding
			// a RunPodSandbox call, allowing it to complete.
			if testStub != nil {
				testStub.Cleanup()
			}

			for _, cleanupID := range cleanupIDs {
				if err := rc.StopPodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", cleanupID, err)
				}

				if err := rc.RemovePodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", cleanupID, err)
				}
			}

			// Reset the Context-scoped state so the next spec never inherits an
			// already-removed sandbox ID or a stopped stub from this one.
			// podConfig is deliberately left alone: a spec that timed out may
			// still have a RunPodSandbox goroutine reading it.
			testStub, podID, joinInFlightRun = nil, "", nil
		})

		It(
			"should not expose sandbox while RunPodSandbox hook is in progress",
			func(ctx SpecContext) {
				// This test validates the spec contract: during RunPodSandbox hook execution,
				// the sandbox MUST NOT be visible via List or Status, and workload containers
				// MUST NOT start.

				// Channel to control blocking behavior
				hookBlocking := make(chan struct{})
				hookReached := make(chan struct{})

				var hookPodID string

				var hookOnce sync.Once

				var err error

				testStub, err = StartNRITestStub("cri-test-nri-block-run", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				// Build the sandbox config up front so the hook below can scope
				// itself to this spec's own sandbox by name. The sandbox has no ID
				// to match on yet, and the name is unique per spec run.
				podSandboxName := "nri-test-block-run-" + framework.NewUUID()
				uid := framework.DefaultUIDPrefix + framework.NewUUID()
				namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
				podConfig = &runtimeapi.PodSandboxConfig{
					Metadata: framework.BuildPodSandboxMetadata(
						podSandboxName,
						uid,
						namespace,
						framework.DefaultAttempt,
					),
					Linux: &runtimeapi.LinuxPodSandboxConfig{
						CgroupParent: common.GetCgroupParent(ctx, rc),
					},
					Labels: framework.DefaultPodLabels,
				}

				// Configure stub to block on RunPodSandbox. sync.Once guards
				// against the hook being invoked more than once, which would
				// otherwise panic on a double close of hookReached and race on
				// hookPodID.
				testStub.Plugin.SetOnRunPodSandbox(
					func(hookCtx context.Context, pod *nri.PodSandbox) error {
						// The plugin sees every sandbox the runtime starts while
						// the stub is connected, including sandboxes created by
						// other actors on the node (a kubelet, say). Blocking one
						// of those would stall an unrelated RunPodSandbox for the
						// length of this spec and publish its ID as hookPodID.
						if pod.GetName() != podSandboxName {
							return nil
						}

						firstInvocation := false

						hookOnce.Do(func() { firstInvocation = true })

						// Skip duplicate invocations so they are not blocked by the
						// test channel handshake.
						if !firstInvocation {
							return nil
						}

						hookPodID = pod.GetId()

						close(hookReached)
						// Block until test signals to continue or context is cancelled (cleanup)
						select {
						case <-hookBlocking:
						case <-hookCtx.Done():
						}

						return nil
					},
				)

				By("triggering RunPodSandbox in a goroutine")

				var (
					runErr      error
					runPodID    string
					runWg       sync.WaitGroup
					releaseOnce sync.Once
				)

				// releaseHook is idempotent so the timeout path, the success path
				// and AfterEach can all unblock the hook safely.
				releaseHook := func() { releaseOnce.Do(func() { close(hookBlocking) }) }

				runWg.Go(func() {
					runPodID, runErr = rc.RunPodSandbox(
						ctx,
						podConfig,
						framework.TestContext.RuntimeHandler,
					)
				})

				// An assertion failure below unwinds straight to AfterEach without
				// joining this goroutine, so hand AfterEach a way to release the
				// hook and wait for RunPodSandbox to return before it issues its
				// stop/remove calls against the sandbox being created.
				joinInFlightRun = func() {
					releaseHook()
					runWg.Wait()
				}

				By("waiting for RunPodSandbox hook to be reached")

				select {
				case <-hookReached:
					// Hook is now blocking
				case <-time.After(30 * time.Second):
					releaseHook() // unblock to avoid goroutine leak
					Fail("Timed out waiting for RunPodSandbox NRI hook to fire")
				}

				// Without a sandbox ID from the hook the checks below would pass
				// vacuously, so fail loudly instead of silently verifying nothing.
				Expect(hookPodID).NotTo(BeEmpty(),
					"RunPodSandbox hook MUST receive a sandbox with a non-empty ID")

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

				blockedResp, blockedErr := rc.PodSandboxStatus(ctx, hookPodID, false)
				// Ideally the sandbox should not be found at all. Some runtimes may
				// return a non-Ready status instead of NotFound — both are acceptable.
				if blockedErr == nil && blockedResp != nil && blockedResp.GetStatus() != nil {
					Expect(
						blockedResp.GetStatus().GetState(),
					).NotTo(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"Sandbox MUST NOT report Ready state while RunPodSandbox hook is in progress")
				}

				By("releasing the hook and verifying pod becomes Ready")
				releaseHook()
				runWg.Wait()

				joinInFlightRun = nil

				Expect(
					runErr,
				).NotTo(HaveOccurred(), "RunPodSandbox should succeed after hook returns")
				Expect(runPodID).NotTo(BeEmpty())
				podID = runPodID

				// After hook completes, sandbox should be Ready
				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"Sandbox should be Ready after RunPodSandbox hook completes")
			},
		)

		It(
			"should not start workload containers until RunPodSandbox hook completes",
			func(ctx SpecContext) {
				// This test validates that workload container creation fails while the
				// RunPodSandbox hook is still running. The NRI hook callback receives the
				// sandbox ID, so we use that to attempt CreateContainer before RunPodSandbox
				// returns. The container creation must fail because the sandbox is not ready.
				hookBlocking := make(chan struct{})
				hookReached := make(chan struct{})

				var (
					err       error
					hookPodID string
					hookOnce  sync.Once
				)

				testStub, err = StartNRITestStub("cri-test-nri-block-container", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				// Build the sandbox config up front so the hook below can scope
				// itself to this spec's own sandbox by name. The sandbox has no ID
				// to match on yet, and the name is unique per spec run.
				podSandboxName := "nri-test-block-container-" + framework.NewUUID()
				uid := framework.DefaultUIDPrefix + framework.NewUUID()
				namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
				podConfig = &runtimeapi.PodSandboxConfig{
					Metadata: framework.BuildPodSandboxMetadata(
						podSandboxName,
						uid,
						namespace,
						framework.DefaultAttempt,
					),
					Linux: &runtimeapi.LinuxPodSandboxConfig{
						CgroupParent: common.GetCgroupParent(ctx, rc),
					},
					Labels: framework.DefaultPodLabels,
				}

				// Configure stub to block RunPodSandbox and capture the sandbox
				// ID. sync.Once guards against the hook being invoked more than
				// once, which would otherwise panic on a double close of
				// hookReached and race on hookPodID.
				testStub.Plugin.SetOnRunPodSandbox(
					func(hookCtx context.Context, pod *nri.PodSandbox) error {
						// The plugin sees every sandbox the runtime starts while
						// the stub is connected, including sandboxes created by
						// other actors on the node (a kubelet, say). Blocking one
						// of those would stall an unrelated RunPodSandbox for the
						// length of this spec and publish its ID as hookPodID,
						// which the CreateContainer attempt below targets.
						if pod.GetName() != podSandboxName {
							return nil
						}

						firstInvocation := false

						hookOnce.Do(func() { firstInvocation = true })

						// Skip duplicate invocations so they are not blocked by the
						// test channel handshake.
						if !firstInvocation {
							return nil
						}

						hookPodID = pod.GetId()

						close(hookReached)

						select {
						case <-hookBlocking:
						case <-hookCtx.Done():
						}

						return nil
					},
				)

				By("pulling the test image before triggering RunPodSandbox")
				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				By("triggering RunPodSandbox in a goroutine")

				var (
					runErr      error
					runPodID    string
					runWg       sync.WaitGroup
					releaseOnce sync.Once
				)

				// releaseHook is idempotent so the timeout path, the success path
				// and AfterEach can all unblock the hook safely.
				releaseHook := func() { releaseOnce.Do(func() { close(hookBlocking) }) }

				runWg.Go(func() {
					runPodID, runErr = rc.RunPodSandbox(
						ctx,
						podConfig,
						framework.TestContext.RuntimeHandler,
					)
				})

				// An assertion failure below unwinds straight to AfterEach without
				// joining this goroutine, so hand AfterEach a way to release the
				// hook and wait for RunPodSandbox to return before it issues its
				// stop/remove calls against the sandbox being created.
				joinInFlightRun = func() {
					releaseHook()
					runWg.Wait()
				}

				By("waiting for RunPodSandbox hook to be reached")

				select {
				case <-hookReached:
					// Hook is now blocking
				case <-time.After(30 * time.Second):
					releaseHook()
					Fail("Timed out waiting for RunPodSandbox NRI hook to fire")
				}

				// An empty ID would make CreateContainer below fail for the
				// trivial "sandbox not found" reason, passing the spec without
				// exercising the RunPodSandbox contract at all.
				Expect(hookPodID).NotTo(BeEmpty(),
					"RunPodSandbox hook MUST receive a sandbox with a non-empty ID")

				By("attempting container creation while RunPodSandbox hook is blocking")

				containerName := "nri-test-while-blocked-" + framework.NewUUID()
				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						containerName,
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image:              framework.TestContext.TestImageList.DefaultTestContainerImage,
						UserSpecifiedImage: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				blockedContainerID, createErr := rc.CreateContainer(
					ctx,
					hookPodID,
					containerConfig,
					podConfig,
				)
				Expect(createErr).To(HaveOccurred(),
					"CreateContainer MUST fail while RunPodSandbox hook is in progress "+
						"(sandbox %s is not ready)", hookPodID)
				Expect(blockedContainerID).To(BeEmpty(),
					"No container ID should be returned when creation fails")

				By("releasing the hook and verifying pod becomes Ready")
				releaseHook()
				runWg.Wait()

				joinInFlightRun = nil

				Expect(
					runErr,
				).NotTo(HaveOccurred(), "RunPodSandbox should succeed after hook returns")
				Expect(runPodID).NotTo(BeEmpty())
				podID = runPodID

				By("verifying container creation succeeds after hook completes")

				containerName = "nri-test-after-hook-" + framework.NewUUID()
				containerConfig = &runtimeapi.ContainerConfig{
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
				containerID := framework.CreateContainer(
					ctx,
					rc,
					ic,
					containerConfig,
					podID,
					podConfig,
				)
				Expect(containerID).NotTo(BeEmpty(),
					"Container creation should succeed after RunPodSandbox hook completes")

				// Clean up container
				if err := rc.StopContainer(ctx, containerID, 0); err != nil {
					framework.Logf("StopContainer(%s) failed: %v", containerID, err)
				}

				if err := rc.RemoveContainer(ctx, containerID); err != nil {
					framework.Logf("RemoveContainer(%s) failed: %v", containerID, err)
				}
			},
		)

		It(
			"should fail RunPodSandbox and clean up when the NRI hook errors, then allow retry",
			func(ctx SpecContext) {
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-run-error", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("building the pod sandbox config")

				// The config is built before the hook is installed so the hook can
				// scope itself to this spec's own sandbox by name.
				podSandboxName := "nri-test-run-error-" + framework.NewUUID()
				uid := framework.DefaultUIDPrefix + framework.NewUUID()
				namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
				podConfig = &runtimeapi.PodSandboxConfig{
					Metadata: framework.BuildPodSandboxMetadata(
						podSandboxName,
						uid,
						namespace,
						framework.DefaultAttempt,
					),
					Linux: &runtimeapi.LinuxPodSandboxConfig{
						CgroupParent: common.GetCgroupParent(ctx, rc),
					},
					Labels: framework.DefaultPodLabels,
				}

				// Fail only the first RunPodSandbox invocation so the retry can pass.
				var failOnce sync.Once

				testStub.Plugin.SetOnRunPodSandbox(
					func(_ context.Context, pod *nri.PodSandbox) error {
						// The plugin sees every sandbox the runtime starts while
						// the stub is connected, including sandboxes created by
						// other actors on the node (a kubelet, say). An unrelated
						// sandbox would otherwise consume failOnce and let the
						// RunPodSandbox below succeed, and would be failed itself.
						if pod.GetName() != podSandboxName {
							return nil
						}

						shouldFail := false

						failOnce.Do(func() { shouldFail = true })

						if shouldFail {
							return errors.New("induced NRI RunPodSandbox failure")
						}

						return nil
					},
				)

				By("attempting RunPodSandbox while the NRI hook is failing")

				failedPodID, runErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				Expect(runErr).To(HaveOccurred(),
					"RunPodSandbox MUST fail when the NRI RunPodSandbox hook returns an error")
				Expect(failedPodID).To(BeEmpty(),
					"No pod sandbox ID should be returned when RunPodSandbox fails")

				By("verifying the NRI RunPodSandbox hook actually fired")
				// The hook records its event before returning the error, so the
				// attempted sandbox ID is available for the cleanup/leak check.
				attemptedID := testStub.Plugin.LastRunPodSandboxID(podSandboxName)
				Expect(attemptedID).NotTo(BeEmpty(),
					"NRI RunPodSandbox hook should have fired before the failure")

				By("verifying the failed sandbox is not left behind")

				pods, listErr := rc.ListPodSandbox(ctx, nil)
				Expect(listErr).NotTo(HaveOccurred())

				for _, pod := range pods {
					matches := pod.GetId() == attemptedID ||
						(pod.GetMetadata() != nil && pod.GetMetadata().GetName() == podSandboxName)
					Expect(matches).To(BeFalse(),
						"sandbox %s MUST be cleaned up after a failed RunPodSandbox", pod.GetId())
				}

				By("retrying RunPodSandbox after the NRI hook stops failing")

				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty(),
					"RunPodSandbox retry should succeed after the NRI hook stops failing")

				By("verifying the retried sandbox becomes Ready")

				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"sandbox should become Ready after a successful RunPodSandbox retry")
			},
		)
	})
})
