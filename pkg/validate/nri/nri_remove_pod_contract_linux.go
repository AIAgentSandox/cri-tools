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
				// RemovePodSandbox called on a running sandbox without a prior
				// StopPodSandbox. The CRI API requires that running containers
				// "must be forcibly terminated and removed". The pod sandbox
				// lifecycle draft in containerd/nri#286 (unmerged, reviewed at
				// revision 2339aa3) places the RemovePodSandbox event after the
				// sandbox is stopped and "just prior to removing the pod from the
				// pod list". So the runtime first does everything StopPodSandbox
				// would do (including its hooks), then removes the containers, and
				// only then delivers RemovePodSandbox: while that hook runs, the
				// pod's containers are gone and the sandbox is still listed as
				// NotReady. A plugin that frees per-pod state on RemovePodSandbox
				// must not see containers of the pod or events for it afterwards.
				//
				// Plugin errors from the RemovePodSandbox hook (the CRI call
				// reporting them, redelivery on retry and withholding removal) are
				// covered by the NRI teardown error-handling tests, not here.
				hookReached := make(chan struct{})

				var (
					hookOnce        sync.Once
					hookTargetPodID atomic.Value
					hookTimedOut    atomic.Bool
				)

				hookTargetPodID.Store("")

				var err error

				testStub, err = StartNRITestStub("cri-test-nri-remove-state", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				// Block the RemovePodSandbox hook for the pod under test only, so
				// the main goroutine can inspect runtime state while it is in
				// progress. sync.Once guards against double close of hookReached
				// on a duplicate invocation. hookTimedOut records whether the
				// runtime's plugin request timeout ended the hook before it was
				// released, which would make the in-hook checks read post-hook
				// state.
				testStub.Plugin.OnRemovePodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
					target, _ := hookTargetPodID.Load().(string)
					if target == "" || pod.GetId() != target {
						return nil
					}

					firstInvocation := false

					hookOnce.Do(func() { firstInvocation = true })

					if !firstInvocation {
						return nil
					}

					close(hookReached)

					select {
					case <-hookBlocking:
					case <-hookCtx.Done():
						hookTimedOut.Store(true)
					}

					return nil
				}

				By("creating a pod sandbox")

				var podConfig *runtimeapi.PodSandboxConfig

				podID, podConfig = removePodContractRunPodSandbox(ctx, rc, "nri-test-remove-state-")
				hookTargetPodID.Store(podID)

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

				var removeErr error

				removeDone := make(chan struct{})

				go func() {
					defer GinkgoRecover()
					defer close(removeDone)

					removeErr = rc.RemovePodSandbox(ctx, podID)
				}()

				By("waiting for RemovePodSandbox hook to be reached")

				select {
				case <-hookReached:
					// Hook is now blocking; main goroutine can inspect state.
				case <-removeDone:
					Fail(
						"RemovePodSandbox returned without delivering the RemovePodSandbox NRI hook " +
							"for the pod under test",
					)
				case <-time.After(30 * time.Second):
					unblockOnce.Do(func() { close(hookBlocking) })
					<-removeDone
					Fail("Timed out waiting for RemovePodSandbox NRI hook to fire")
				}

				By("observing runtime state while the hook is blocked")

				// Gather the observations first and keep this fast: the runtime's
				// plugin request timeout (about 2s by default in containerd) ends
				// the hook, after which these calls would read post-hook state.
				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				sandboxes, listPodErr := rc.ListPodSandbox(
					ctx,
					&runtimeapi.PodSandboxFilter{Id: podID},
				)

				removeReturnedDuringHook := false

				select {
				case <-removeDone:
					removeReturnedDuringHook = true
				default:
				}

				Expect(hookTimedOut.Load()).To(BeFalse(),
					"invalid test run: the RemovePodSandbox hook context expired before the hook was "+
						"released, so the in-hook checks may have observed post-hook state")

				By("verifying RemovePodSandbox had not returned while the hook was blocked")
				Expect(removeReturnedDuringHook).To(BeFalse(),
					"RemovePodSandbox MUST NOT return while the RemovePodSandbox NRI hook is in progress")

				By("verifying all workload containers were removed before the hook fired")
				Expect(listErr).NotTo(HaveOccurred(), "ListContainers during RemovePodSandbox hook")
				Expect(containers).To(BeEmpty(),
					"all workload containers MUST be removed before RemovePodSandbox hook fires")

				By(
					"verifying the sandbox is still listed and stopped (NOT_READY) while the hook is in progress",
				)
				Expect(
					listPodErr,
				).NotTo(HaveOccurred(), "ListPodSandbox during RemovePodSandbox hook")
				Expect(sandboxes).To(HaveLen(1),
					"sandbox MUST still be listed while the RemovePodSandbox NRI hook is in progress")
				Expect(
					sandboxes[0].GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"StopPodSandbox MUST have completed before RemovePodSandbox hook fires")

				By("releasing the hook and verifying RemovePodSandbox succeeds")
				unblockOnce.Do(func() { close(hookBlocking) })
				<-removeDone
				Expect(
					removeErr,
				).NotTo(HaveOccurred(), "RemovePodSandbox should succeed after hook returns")

				removedPodID, removedContainerID := podID, containerID

				By("verifying the sandbox is no longer listed")

				sandboxes, listPodErr = rc.ListPodSandbox(
					ctx,
					&runtimeapi.PodSandboxFilter{Id: removedPodID},
				)
				Expect(listPodErr).NotTo(HaveOccurred())
				Expect(sandboxes).To(BeEmpty(),
					"sandbox MUST NOT be listed by ListPodSandbox after RemovePodSandbox returns")

				// RemovePodSandbox removed both; clear so AfterEach skips them.
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
				Expect(stopCtrIdx).To(BeNumerically("<", removeCtrIdx),
					"StopContainer MUST precede RemoveContainer for the workload container")
				Expect(stopCtrIdx).To(BeNumerically("<", stopPodIdx),
					"workload containers MUST be stopped before StopPodSandbox")
				Expect(stopPodIdx).To(BeNumerically("<", removeCtrIdx),
					"StopPodSandbox MUST precede RemoveContainer when RemovePodSandbox is called on a running sandbox")
				Expect(stopPodIdx).To(BeNumerically("<", removePodIdx),
					"StopPodSandbox MUST precede RemovePodSandbox")
				Expect(removeCtrIdx).To(BeNumerically("<", removePodIdx),
					"workload containers MUST be removed before RemovePodSandbox")

				By("verifying the RemovePodSandbox NRI hook was delivered exactly once")
				Expect(
					removePodContractCountEvents(
						testStub.Plugin,
						removedPodID,
						EventRemovePodSandbox,
					),
				).
					To(Equal(1), "RemovePodSandbox NRI hook MUST be delivered exactly once")
			},
		)

		It(
			"should remove the sandbox from ListPodSandbox and handle a second RemovePodSandbox without another NRI event",
			func(ctx SpecContext) {
				// After RemovePodSandbox returns the sandbox is no longer listed.
				// The CRI API makes RemovePodSandbox idempotent: it "must not
				// return an error if the sandbox has already been removed". A
				// repeated call on a removed pod must not deliver any NRI event
				// for it: plugins have already released that pod and would
				// otherwise see teardown for an object that no longer exists.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-remove-idempotent", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox")

				podID, _ = removePodContractRunPodSandbox(ctx, rc, "nri-test-remove-idempotent-")

				By("calling RemovePodSandbox the first time")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"First RemovePodSandbox call should succeed")

				removedPodID := podID

				By("verifying the sandbox is no longer listed")

				sandboxes, listErr := rc.ListPodSandbox(
					ctx,
					&runtimeapi.PodSandboxFilter{Id: removedPodID},
				)
				Expect(listErr).NotTo(HaveOccurred())
				Expect(sandboxes).To(BeEmpty(),
					"sandbox MUST NOT be listed by ListPodSandbox after RemovePodSandbox returns")

				// The sandbox is gone; clear so AfterEach skips it.
				podID = ""

				By("verifying the RemovePodSandbox NRI hook fired exactly once")
				Eventually(func() int {
					return removePodContractCountEvents(
						testStub.Plugin,
						removedPodID,
						EventRemovePodSandbox,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"RemovePodSandbox NRI hook should fire exactly once for the first call")

				eventsAfterFirstRemove := len(
					FilterEventsByPodID(testStub.Plugin.Events(), removedPodID),
				)

				By("calling RemovePodSandbox again (idempotency check)")
				Expect(rc.RemovePodSandbox(ctx, removedPodID)).NotTo(HaveOccurred(),
					"Second RemovePodSandbox call MUST succeed (idempotent)")

				By(
					"verifying the second RemovePodSandbox does NOT deliver any NRI event for the pod",
				)
				Consistently(func() int {
					return len(FilterEventsByPodID(testStub.Plugin.Events(), removedPodID))
				}, 2*time.Second, 200*time.Millisecond).Should(Equal(eventsAfterFirstRemove),
					"Second RemovePodSandbox MUST NOT deliver any NRI event for the already removed pod")

				By("verifying the plugin still receives events (positive control)")

				podID, _ = removePodContractRunPodSandbox(
					ctx,
					rc,
					"nri-test-remove-idempotent-control-",
				)
				controlPodID := podID

				Eventually(func() int {
					return removePodContractCountEvents(
						testStub.Plugin,
						controlPodID,
						EventRunPodSandbox,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"the plugin MUST still receive events after the second RemovePodSandbox")
			},
		)
	})
})
