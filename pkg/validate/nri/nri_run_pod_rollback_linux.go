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
	"slices"
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
	// rollbackObserveTimeout bounds how long the tests wait for teardown
	// notifications of a rolled-back object. Runtimes deliver them (if at all)
	// while unwinding the failed CRI call, so a short window is sufficient.
	rollbackObserveTimeout = 5 * time.Second
	rollbackObservePoll    = 200 * time.Millisecond
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

	Context("RunPodSandbox rollback", Serial, func() {
		var (
			stubs []*NRITestStub
			// podIDs holds every sandbox ID the spec created or attempted, so
			// AfterEach can remove anything a failed rollback left behind.
			podIDs []string
		)

		BeforeEach(func() {
			stubs = nil
			podIDs = nil
		})

		AfterEach(func(ctx SpecContext) {
			// Stop the stubs first so a still-failing hook cannot interfere with
			// teardown of the sandboxes.
			for _, s := range stubs {
				s.Cleanup()
			}

			for _, id := range podIDs {
				if err := rc.StopPodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", id, err)
				}

				if err := rc.RemovePodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", id, err)
				}
			}
		})

		It(
			"should notify a plugin that accepted RunPodSandbox when a higher-index plugin rejects it",
			func(ctx SpecContext) {
				By("starting an accepting plugin at index 00 and a rejecting plugin at index 10")

				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-accept", idx: "00"},
					rollbackStubSpec{name: "cri-test-nri-rollback-reject", idx: "10"},
				)
				accepting, rejecting := stubs[0].Plugin, stubs[1].Plugin

				rejecting.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return errors.New("induced NRI RunPodSandbox rejection")
				}

				By("attempting RunPodSandbox")

				podConfig, podName := newRollbackPodConfig(ctx, rc, "nri-test-rollback-multi-")
				failedID, runErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				Expect(runErr).To(HaveOccurred(),
					"RunPodSandbox MUST fail when an NRI plugin rejects the RunPodSandbox event")
				Expect(failedID).To(BeEmpty(),
					"no pod sandbox ID should be returned when RunPodSandbox fails")

				attemptedID := accepting.LastRunPodSandboxID()
				Expect(attemptedID).NotTo(BeEmpty(),
					"the accepting plugin should have received RunPodSandbox before the rejection")

				podIDs = append(podIDs, attemptedID)

				By("verifying both plugins saw the same sandbox in plugin index order")

				Expect(rejecting.LastRunPodSandboxID()).To(Equal(attemptedID),
					"the rejecting plugin should have received RunPodSandbox for the same sandbox")

				acceptRun := rollbackFindEvent(
					accepting.Events(), EventRunPodSandbox, attemptedID, "",
				)
				rejectRun := rollbackFindEvent(
					rejecting.Events(), EventRunPodSandbox, attemptedID, "",
				)

				Expect(acceptRun).NotTo(BeNil())
				Expect(rejectRun).NotTo(BeNil())

				Expect(acceptRun.Timestamp).NotTo(BeTemporally(">", rejectRun.Timestamp),
					"RunPodSandbox hooks MUST execute in plugin index order "+
						"(index 00 before index 10)")

				By("verifying the failed sandbox is not left behind")
				expectRollbackPodNotListed(ctx, rc, attemptedID, podName)

				By("checking whether the accepting plugin is told the sandbox was rolled back")

				if !rollbackTeardownObserved(accepting, attemptedID, "", EventRemovePodSandbox) {
					Skip(
						"Expected: the plugin at index 00, which accepted RunPodSandbox for sandbox " +
							attemptedID + ", receives StopPodSandbox and RemovePodSandbox for that sandbox " +
							"after the plugin at index 10 rejected it, so the accepting plugin can release " +
							"any state it allocated for the sandbox. " +
							"Observed: no teardown event was delivered (events: " +
							rollbackEventTypes(
								accepting.Events(),
								attemptedID,
								"",
							) + "). " +
							"The NRI pod sandbox lifecycle spec (containerd/nri PR 286, " +
							"docs/pod-sandbox-lifecycle.md) does not state what plugins are told when a " +
							"later plugin fails RunPodSandbox, so runtimes are not required to do this yet. " +
							"containerd (main, release/2.3) does: when nri.RunPodSandbox fails it relays " +
							"StopPodSandbox and RemovePodSandbox for the sandbox to every plugin. " +
							"See https://github.com/kubernetes-sigs/cri-tools/issues/2046.",
					)
				}

				teardown := rollbackTeardownEvents(accepting.Events(), attemptedID, "",
					EventStopPodSandbox, EventRemovePodSandbox)
				Expect(
					teardown,
				).To(Equal([]NRIEventType{EventStopPodSandbox, EventRemovePodSandbox}),
					"rollback notifications MUST follow the RunPodSandbox -> StopPodSandbox -> "+
						"RemovePodSandbox order, each delivered once")
			},
		)

		It(
			"should fail RunPodSandbox without leaking when the only plugin rejects it",
			func(ctx SpecContext) {
				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-self", idx: "00"},
				)
				plugin := stubs[0].Plugin

				plugin.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return errors.New("induced NRI RunPodSandbox rejection")
				}

				By("attempting RunPodSandbox")

				podConfig, podName := newRollbackPodConfig(ctx, rc, "nri-test-rollback-self-")
				failedID, runErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				Expect(runErr).To(HaveOccurred(),
					"RunPodSandbox MUST fail when the NRI RunPodSandbox hook returns an error")
				Expect(failedID).To(BeEmpty(),
					"no pod sandbox ID should be returned when RunPodSandbox fails")

				attemptedID := plugin.LastRunPodSandboxID()
				Expect(attemptedID).NotTo(BeEmpty(),
					"the plugin should have received RunPodSandbox before rejecting it")

				podIDs = append(podIDs, attemptedID)

				By("verifying the failed sandbox is not left behind")
				expectRollbackPodNotListed(ctx, rc, attemptedID, podName)

				By(
					"recording whether the rejecting plugin receives teardown events for the sandbox",
				)

				notified := rollbackTeardownObserved(plugin, attemptedID, "", EventRemovePodSandbox)
				teardown := rollbackTeardownEvents(plugin.Events(), attemptedID, "",
					EventStopPodSandbox, EventRemovePodSandbox)

				if notified {
					Expect(
						teardown,
					).To(Equal([]NRIEventType{EventStopPodSandbox, EventRemovePodSandbox}),
						"if the runtime notifies the rejecting plugin, notifications MUST follow the "+
							"RunPodSandbox -> StopPodSandbox -> RemovePodSandbox order, each delivered once")
				}

				Skip("The NRI pod sandbox lifecycle spec (containerd/nri PR 286, " +
					"docs/pod-sandbox-lifecycle.md) only says plugins \"bear responsibility for errors " +
					"they return\" from RunPodSandbox; it does not say whether the rejecting plugin " +
					"receives StopPodSandbox/RemovePodSandbox for the rejected sandbox, so delivery is " +
					"not asserted (only their order, if delivered). Sandbox failure and cleanup were " +
					"verified. " +
					"Observed teardown events for sandbox " + attemptedID + ": " +
					fmt.Sprint(teardown) + ". " +
					"containerd (main, release/2.3) relays StopPodSandbox and RemovePodSandbox to every " +
					"plugin, including the rejecting one, when nri.RunPodSandbox fails. " +
					"See https://github.com/kubernetes-sigs/cri-tools/issues/2046.")
			},
		)

		It(
			"should notify the plugin with StopContainer and RemoveContainer when it rejects CreateContainer",
			func(ctx SpecContext) {
				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-ctr", idx: "00"},
				)
				plugin := stubs[0].Plugin

				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				By("creating a pod sandbox")

				podConfig, _ := newRollbackPodConfig(ctx, rc, "nri-test-rollback-ctr-")
				podID := framework.RunPodSandbox(ctx, rc, podConfig)
				podIDs = append(podIDs, podID)

				plugin.OnCreateContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					return errors.New("induced NRI CreateContainer rejection")
				}

				By("attempting CreateContainer")

				containerName := "nri-test-rollback-ctr-" + framework.NewUUID()
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

				failedID, createErr := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(createErr).To(HaveOccurred(),
					"CreateContainer MUST fail when the NRI CreateContainer hook returns an error")
				Expect(failedID).To(BeEmpty(),
					"no container ID should be returned when CreateContainer fails")

				createEvent := rollbackLastEvent(plugin.Events(), EventCreateContainer)
				Expect(createEvent).NotTo(BeNil(),
					"the plugin should have received CreateContainer before rejecting it")

				attemptedID := createEvent.ContainerID
				Expect(attemptedID).NotTo(BeEmpty())

				By("verifying the failed container is not left behind")

				containers, err := rc.ListContainers(
					ctx,
					&runtimeapi.ContainerFilter{PodSandboxId: podID},
				)
				Expect(err).NotTo(HaveOccurred())

				for _, c := range containers {
					matches := c.GetId() == attemptedID ||
						(c.GetMetadata() != nil && c.GetMetadata().GetName() == containerName)
					Expect(matches).To(BeFalse(),
						"container %s MUST be cleaned up after a failed CreateContainer", c.GetId())
				}

				By("checking whether the plugin is told the container was rolled back")

				if !rollbackTeardownObserved(plugin, podID, attemptedID, EventRemoveContainer) {
					Skip("Expected: after the plugin rejects CreateContainer for container " +
						attemptedID + ", the runtime delivers StopContainer and RemoveContainer for " +
						"that container so plugin state stays consistent with the runtime. " +
						"Observed: no teardown event was delivered (events: " +
						rollbackEventTypes(plugin.Events(), podID, attemptedID) + "). " +
						"No NRI spec states this for CreateContainer failures, so runtimes are not " +
						"required to do it. containerd (main, release/2.3) does: it calls " +
						"UndoCreateContainer, which relays StopContainer and RemoveContainer to all " +
						"plugins. See https://github.com/kubernetes-sigs/cri-tools/issues/2046.")
				}

				teardown := rollbackTeardownEvents(plugin.Events(), podID, attemptedID,
					EventStopContainer, EventRemoveContainer)
				Expect(teardown).To(Equal([]NRIEventType{EventStopContainer, EventRemoveContainer}),
					"rollback notifications MUST follow the CreateContainer -> StopContainer -> "+
						"RemoveContainer order, each delivered once")
			},
		)
	})
})

// rollbackStubSpec names one NRI test plugin started by startRollbackStubs.
type rollbackStubSpec struct {
	name string
	idx  string
}

// startRollbackStubs starts one NRI test stub per spec, in order. On failure
// it stops the stubs already started before failing the spec.
func startRollbackStubs(specs ...rollbackStubSpec) []*NRITestStub {
	stubs := make([]*NRITestStub, 0, len(specs))

	for _, spec := range specs {
		s, err := StartNRITestStub(spec.name, spec.idx)
		if err != nil {
			for _, started := range stubs {
				started.Cleanup()
			}

			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub %s", spec.name)
		}

		stubs = append(stubs, s)
	}

	return stubs
}

// newRollbackPodConfig builds a uniquely named pod sandbox config and returns
// it together with the sandbox name.
func newRollbackPodConfig(
	ctx context.Context,
	rc internalapi.RuntimeService,
	prefix string,
) (config *runtimeapi.PodSandboxConfig, name string) {
	name = prefix + framework.NewUUID()
	uid := framework.DefaultUIDPrefix + framework.NewUUID()
	namespace := framework.DefaultNamespacePrefix + framework.NewUUID()

	return &runtimeapi.PodSandboxConfig{
		Metadata: framework.BuildPodSandboxMetadata(name, uid, namespace, framework.DefaultAttempt),
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			CgroupParent: common.GetCgroupParent(ctx, rc),
		},
		Labels: framework.DefaultPodLabels,
	}, name
}

// expectRollbackPodNotListed asserts that no sandbox with the given ID or name
// is listed by the runtime.
func expectRollbackPodNotListed(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podID, podName string,
) {
	pods, err := rc.ListPodSandbox(ctx, nil)
	Expect(err).NotTo(HaveOccurred())

	for _, pod := range pods {
		matches := pod.GetId() == podID ||
			(pod.GetMetadata() != nil && pod.GetMetadata().GetName() == podName)
		Expect(matches).To(BeFalse(),
			"sandbox %s MUST be cleaned up after a failed RunPodSandbox", pod.GetId())
	}
}

// rollbackMatches reports whether the event targets the given sandbox and,
// when containerID is non-empty, the given container.
func rollbackMatches(e *NRIEvent, podID, containerID string) bool {
	if e.PodSandboxID != podID {
		return false
	}

	return containerID == "" || e.ContainerID == containerID
}

// rollbackFindEvent returns the first event of the given type for the sandbox
// (and container, if non-empty), or nil.
func rollbackFindEvent(events []NRIEvent, t NRIEventType, podID, containerID string) *NRIEvent {
	for i := range events {
		if events[i].Type == t && rollbackMatches(&events[i], podID, containerID) {
			return &events[i]
		}
	}

	return nil
}

// rollbackLastEvent returns the most recent event of the given type, or nil.
func rollbackLastEvent(events []NRIEvent, t NRIEventType) *NRIEvent {
	for i := range slices.Backward(events) {
		if events[i].Type == t {
			return &events[i]
		}
	}

	return nil
}

// rollbackTeardownEvents returns, in delivery order, the events of the given
// types the plugin received for the sandbox (and container, if non-empty).
func rollbackTeardownEvents(
	events []NRIEvent,
	podID, containerID string,
	types ...NRIEventType,
) []NRIEventType {
	var result []NRIEventType

	for i := range events {
		if slices.Contains(types, events[i].Type) &&
			rollbackMatches(&events[i], podID, containerID) {
			result = append(result, events[i].Type)
		}
	}

	return result
}

// rollbackTeardownObserved waits for the plugin to receive an event of the
// given type for the sandbox (and container, if non-empty). It does not fail
// the spec on timeout, so a missing notification can be reported via Skip.
func rollbackTeardownObserved(p *NRITestPlugin, podID, containerID string, want NRIEventType) bool {
	g := NewGomega(func(string, ...int) {})

	return g.Eventually(func() []NRIEventType {
		return rollbackTeardownEvents(p.Events(), podID, containerID, want)
	}).WithTimeout(rollbackObserveTimeout).WithPolling(rollbackObservePoll).Should(ContainElement(want))
}

// rollbackEventTypes renders all event types recorded for the sandbox (and
// container, if non-empty), for use in Skip notes.
func rollbackEventTypes(events []NRIEvent, podID, containerID string) string {
	var types []NRIEventType

	for i := range events {
		if rollbackMatches(&events[i], podID, containerID) {
			types = append(types, events[i].Type)
		}
	}

	return fmt.Sprint(types)
}
