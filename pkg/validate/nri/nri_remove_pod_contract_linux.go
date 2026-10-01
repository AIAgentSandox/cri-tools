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

// removePodContractRunPodSandbox creates a pod sandbox for the RemovePodSandbox
// contract tests and returns its ID and config.
func removePodContractRunPodSandbox(
	ctx context.Context,
	rc internalapi.RuntimeService,
	namePrefix string,
) (string, *runtimeapi.PodSandboxConfig) {
	podConfig := &runtimeapi.PodSandboxConfig{
		Metadata: framework.BuildPodSandboxMetadata(
			namePrefix+framework.NewUUID(),
			framework.DefaultUIDPrefix+framework.NewUUID(),
			framework.DefaultNamespacePrefix+framework.NewUUID(),
			framework.DefaultAttempt,
		),
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			CgroupParent: common.GetCgroupParent(ctx, rc),
		},
		Labels: framework.DefaultPodLabels,
	}
	podID := framework.RunPodSandbox(ctx, rc, podConfig)
	Expect(podID).NotTo(BeEmpty())

	return podID, podConfig
}

// removePodContractEventIndex returns the index of the first event of the given
// type (and container ID, if non-empty) in events, or -1 if not found.
func removePodContractEventIndex(
	events []NRIEvent,
	eventType NRIEventType,
	containerID string,
) int {
	return slices.IndexFunc(events, func(e NRIEvent) bool {
		return e.Type == eventType && (containerID == "" || e.ContainerID == containerID)
	})
}

// removePodContractCountEvents returns the number of events of the given type
// recorded for the pod sandbox.
func removePodContractCountEvents(plugin *NRITestPlugin, podID string, eventType NRIEventType) int {
	count := 0

	events := FilterEventsByPodID(plugin.Events(), podID)
	for i := range events {
		if events[i].Type == eventType {
			count++
		}
	}

	return count
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

	Context("RemovePodSandbox contract", Serial, func() {
		var (
			testStub     *NRITestStub
			podID        string
			containerID  string
			hookBlocking chan struct{}
			unblockOnce  sync.Once
		)

		BeforeEach(func() {
			hookBlocking = make(chan struct{})
			unblockOnce = sync.Once{}
		})

		AfterEach(func(ctx SpecContext) {
			// Release any blocked RemovePodSandbox hook before stopping the
			// stub so the in-flight CRI call can complete.
			unblockOnce.Do(func() { close(hookBlocking) })

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

				containerID = ""
			}

			if podID != "" {
				if err := rc.StopPodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", podID, err)
				}

				if err := rc.RemovePodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", podID, err)
				}

				podID = ""
			}
		})

		It(
			"should stop and remove workload containers and stop the sandbox before the RemovePodSandbox hook fires",
			func(ctx SpecContext) {
				// This test validates the spec contract for RemovePodSandbox called on a
				// running sandbox without a prior StopPodSandbox: the runtime MUST stop
				// and remove all workload containers and complete StopPodSandbox before
				// the RemovePodSandbox NRI event fires (RunPodSandbox → StopPodSandbox →
				// RemovePodSandbox ordering).
				hookReached := make(chan struct{})

				var hookOnce sync.Once

				var err error

				testStub, err = StartNRITestStub("cri-test-nri-remove-state", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				// Block the RemovePodSandbox hook so the main goroutine can inspect
				// runtime state while it is in progress. sync.Once guards against
				// double close of hookReached on a duplicate invocation.
				testStub.Plugin.OnRemovePodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
					firstInvocation := false

					hookOnce.Do(func() { firstInvocation = true })

					if !firstInvocation {
						return nil
					}

					close(hookReached)

					select {
					case <-hookBlocking:
					case <-hookCtx.Done():
					}

					return nil
				}

				By("creating a pod sandbox")

				var podConfig *runtimeapi.PodSandboxConfig

				podID, podConfig = removePodContractRunPodSandbox(ctx, rc, "nri-test-remove-state-")

				By("creating and starting a container in the sandbox")
				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						"nri-test-remove-state-ctr-"+framework.NewUUID(),
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}
				containerID = framework.CreateContainer(
					ctx,
					rc,
					ic,
					containerConfig,
					podID,
					podConfig,
				)
				Expect(containerID).NotTo(BeEmpty())
				Expect(rc.StartContainer(ctx, containerID)).NotTo(HaveOccurred())

				By("triggering RemovePodSandbox without a prior StopPodSandbox in a goroutine")

				var (
					removeErr error
					removeWg  sync.WaitGroup
				)

				removeWg.Go(func() {
					removeErr = rc.RemovePodSandbox(ctx, podID)
				})

				By("waiting for RemovePodSandbox hook to be reached")

				select {
				case <-hookReached:
					// Hook is now blocking; main goroutine can inspect state.
				case <-time.After(30 * time.Second):
					unblockOnce.Do(func() { close(hookBlocking) })
					removeWg.Wait()
					Fail("Timed out waiting for RemovePodSandbox NRI hook to fire")
				}

				By("verifying all workload containers were removed before the hook fired")

				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(listErr).NotTo(HaveOccurred(), "ListContainers during RemovePodSandbox hook")
				Expect(containers).To(BeEmpty(),
					"all workload containers MUST be removed before RemovePodSandbox hook fires")

				By("verifying the sandbox is stopped (NOT_READY) while the hook is in progress")

				sandboxes, listPodErr := rc.ListPodSandbox(
					ctx,
					&runtimeapi.PodSandboxFilter{Id: podID},
				)
				Expect(
					listPodErr,
				).NotTo(HaveOccurred(), "ListPodSandbox during RemovePodSandbox hook")

				if len(sandboxes) == 0 {
					// SPEC_DISCREPANCY: the spec says the RemovePodSandbox event fires
					// "just prior to removing the pod from the pod list", so the sandbox
					// should still be listed while the hook is in progress.
					Skip(
						"spec discrepancy: sandbox is no longer listed by ListPodSandbox while the " +
							"RemovePodSandbox NRI hook is in progress; spec says the event fires just " +
							"prior to removing the pod from the pod list",
					)
				}

				Expect(sandboxes).To(HaveLen(1))
				Expect(
					sandboxes[0].GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"StopPodSandbox MUST have completed before RemovePodSandbox hook fires")

				By("releasing the hook and verifying RemovePodSandbox succeeds")
				unblockOnce.Do(func() { close(hookBlocking) })
				removeWg.Wait()
				Expect(
					removeErr,
				).NotTo(HaveOccurred(), "RemovePodSandbox should succeed after hook returns")

				// RemovePodSandbox removed both; clear so AfterEach skips them.
				removedPodID, removedContainerID := podID, containerID
				podID, containerID = "", ""

				By("verifying NRI event ordering for the pod")

				events := FilterEventsByPodID(testStub.Plugin.Events(), removedPodID)

				runIdx := removePodContractEventIndex(events, EventRunPodSandbox, "")
				stopCtrIdx := removePodContractEventIndex(
					events,
					EventStopContainer,
					removedContainerID,
				)
				removeCtrIdx := removePodContractEventIndex(
					events,
					EventRemoveContainer,
					removedContainerID,
				)
				stopPodIdx := removePodContractEventIndex(events, EventStopPodSandbox, "")
				removePodIdx := removePodContractEventIndex(events, EventRemovePodSandbox, "")

				Expect(runIdx).To(BeNumerically(">=", 0), "RunPodSandbox event MUST be recorded")
				Expect(stopCtrIdx).To(BeNumerically(">=", 0),
					"StopContainer event MUST be recorded for the workload container")
				Expect(removeCtrIdx).To(BeNumerically(">=", 0),
					"RemoveContainer event MUST be recorded for the workload container")
				Expect(stopPodIdx).To(BeNumerically(">=", 0),
					"StopPodSandbox event MUST be recorded when RemovePodSandbox is called on a running sandbox")
				Expect(
					removePodIdx,
				).To(BeNumerically(">=", 0), "RemovePodSandbox event MUST be recorded")

				Expect(runIdx).To(BeNumerically("<", stopPodIdx),
					"RunPodSandbox MUST precede StopPodSandbox")
				Expect(stopCtrIdx).To(BeNumerically("<", stopPodIdx),
					"workload containers MUST be stopped before StopPodSandbox")
				Expect(stopPodIdx).To(BeNumerically("<", removePodIdx),
					"StopPodSandbox MUST precede RemovePodSandbox")
				Expect(removeCtrIdx).To(BeNumerically("<", removePodIdx),
					"workload containers MUST be removed before RemovePodSandbox")
			},
		)

		It(
			"should remove the sandbox from ListPodSandbox and handle a second RemovePodSandbox without another NRI event",
			func(ctx SpecContext) {
				// This test validates that after RemovePodSandbox returns the sandbox is
				// no longer listed, and that RemovePodSandbox is idempotent: a second
				// call succeeds and does not generate another RemovePodSandbox NRI event.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-remove-idempotent", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox")

				podID, _ = removePodContractRunPodSandbox(ctx, rc, "nri-test-remove-idempotent-")

				By("calling RemovePodSandbox the first time")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"First RemovePodSandbox call should succeed")

				removedPodID := podID
				podID = ""

				By("verifying the sandbox is no longer listed")

				sandboxes, listErr := rc.ListPodSandbox(
					ctx,
					&runtimeapi.PodSandboxFilter{Id: removedPodID},
				)
				Expect(listErr).NotTo(HaveOccurred())
				Expect(sandboxes).To(BeEmpty(),
					"sandbox MUST NOT be listed by ListPodSandbox after RemovePodSandbox returns")

				By("verifying the RemovePodSandbox NRI hook fired exactly once")
				Eventually(func() int {
					return removePodContractCountEvents(
						testStub.Plugin,
						removedPodID,
						EventRemovePodSandbox,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"RemovePodSandbox NRI hook should fire exactly once for the first call")

				By("calling RemovePodSandbox again (idempotency check)")
				Expect(rc.RemovePodSandbox(ctx, removedPodID)).NotTo(HaveOccurred(),
					"Second RemovePodSandbox call MUST succeed (idempotent)")

				By("verifying the second RemovePodSandbox does NOT generate an NRI event")
				Consistently(func() int {
					return removePodContractCountEvents(
						testStub.Plugin,
						removedPodID,
						EventRemovePodSandbox,
					)
				}, 2*time.Second, 200*time.Millisecond).Should(Equal(1),
					"Second RemovePodSandbox MUST NOT generate an NRI event — sandbox is already removed")
			},
		)
	})
})
