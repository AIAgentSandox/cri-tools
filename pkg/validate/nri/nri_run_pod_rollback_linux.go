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
	"sync/atomic"

	nri "github.com/containerd/nri/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

const (
	// rollbackRationale is the expectation behind every rollback assertion in
	// this file, with the issue that asked for it.
	rollbackRationale = "a plugin whose create hook ran must be told, before the CRI call returns, " +
		"that the object was rolled back, or it leaks whatever it allocated for it " +
		"(https://github.com/kubernetes-sigs/cri-tools/issues/2046)"

	// rollbackRetryRationale is the expectation behind the same-metadata retry.
	rollbackRetryRationale = "a failed create must leave nothing behind, so an immediate retry with " +
		"the same metadata succeeds"
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

	// When a create fails after some plugin's create hook was entered, every
	// plugin whose hook was entered, including the one that failed, gets the
	// matching teardown in order before the CRI call returns. The reason: a
	// plugin that allocated on RunPodSandbox or CreateContainer must be told
	// when the object is rolled back, or it leaks. See
	// https://github.com/kubernetes-sigs/cri-tools/issues/2046.
	//
	// The teardown is checked on a snapshot of the events taken right when the
	// CRI call returns, and at least once each: a failed create is not a
	// successful operation, so a repeated rollback teardown is allowed.
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

		registerPod := func(id string) {
			if id != "" && !slices.Contains(podIDs, id) {
				podIDs = append(podIDs, id)
			}
		}

		It(
			"should fail RunPodSandbox without leaking and notify both plugins "+
				"when a higher-index plugin rejects it",
			func(ctx SpecContext) {
				By("starting an accepting plugin at index 00 and a rejecting plugin at index 10")

				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-accept", idx: "00"},
					rollbackStubSpec{name: "cri-test-nri-rollback-reject", idx: "10"},
				)
				accepting, rejecting := stubs[0].Plugin, stubs[1].Plugin

				var reject atomic.Bool
				reject.Store(true)

				rejecting.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					if reject.Load() {
						return errors.New("induced NRI RunPodSandbox rejection")
					}

					return nil
				}

				By("attempting RunPodSandbox")

				podConfig, podName := newRollbackPodConfig(ctx, rc, "nri-test-rollback-multi-")
				failedID, runErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				// Snapshot right when the call returns: the rollback teardown
				// must already have been delivered.
				acceptEvents, rejectEvents := accepting.Events(), rejecting.Events()

				attemptedID := accepting.LastRunPodSandboxID()

				registerPod(failedID)
				registerPod(attemptedID)

				Expect(runErr).To(HaveOccurred(),
					"RunPodSandbox MUST fail when an NRI plugin rejects the RunPodSandbox event")
				Expect(failedID).To(BeEmpty(),
					"no pod sandbox ID should be returned when RunPodSandbox fails")
				Expect(attemptedID).NotTo(BeEmpty(),
					"the accepting plugin should have received RunPodSandbox before the rejection")

				By("verifying both plugins saw the same sandbox in plugin index order")

				Expect(rejecting.LastRunPodSandboxID()).To(Equal(attemptedID),
					"the rejecting plugin should have received RunPodSandbox for the same sandbox")

				acceptRun := rollbackFindEvent(acceptEvents, EventRunPodSandbox, attemptedID, "")
				rejectRun := rollbackFindEvent(rejectEvents, EventRunPodSandbox, attemptedID, "")

				Expect(acceptRun).NotTo(BeNil())
				Expect(rejectRun).NotTo(BeNil())

				Expect(acceptRun.Timestamp).NotTo(BeTemporally(">", rejectRun.Timestamp),
					"RunPodSandbox hooks MUST execute in plugin index order (index 00 before "+
						"index 10), per the unmerged draft containerd/nri#286 at revision 2339aa3")

				By("verifying the failed sandbox is not listed when the call returns")
				expectRollbackPodNotListed(ctx, rc, attemptedID, podName)

				By("verifying both plugins got StopPodSandbox then RemovePodSandbox " +
					"before the call returned")
				expectRollbackPodTeardown(
					acceptEvents,
					"the accepting plugin at index 00",
					attemptedID,
				)
				expectRollbackPodTeardown(
					rejectEvents,
					"the rejecting plugin at index 10",
					attemptedID,
				)

				By("retrying RunPodSandbox with the same metadata")

				reject.Store(false)
				retryRollbackPod(ctx, rc, podConfig, registerPod)
			},
		)

		It(
			"should fail RunPodSandbox without leaking and notify the plugin when the only plugin rejects it",
			func(ctx SpecContext) {
				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-self", idx: "00"},
				)
				plugin := stubs[0].Plugin

				var reject atomic.Bool
				reject.Store(true)

				plugin.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					if reject.Load() {
						return errors.New("induced NRI RunPodSandbox rejection")
					}

					return nil
				}

				By("attempting RunPodSandbox")

				podConfig, podName := newRollbackPodConfig(ctx, rc, "nri-test-rollback-self-")
				failedID, runErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				events := plugin.Events()

				attemptedID := plugin.LastRunPodSandboxID()

				registerPod(failedID)
				registerPod(attemptedID)

				Expect(runErr).To(HaveOccurred(),
					"RunPodSandbox MUST fail when the NRI RunPodSandbox hook returns an error")
				Expect(failedID).To(BeEmpty(),
					"no pod sandbox ID should be returned when RunPodSandbox fails")
				Expect(attemptedID).NotTo(BeEmpty(),
					"the plugin should have received RunPodSandbox before rejecting it")

				By("verifying the failed sandbox is not listed when the call returns")
				expectRollbackPodNotListed(ctx, rc, attemptedID, podName)

				By(
					"verifying the rejecting plugin got StopPodSandbox then RemovePodSandbox before the call returned",
				)
				expectRollbackPodTeardown(events, "the rejecting plugin", attemptedID)

				By("retrying RunPodSandbox with the same metadata")

				reject.Store(false)
				retryRollbackPod(ctx, rc, podConfig, registerPod)
			},
		)

		It(
			"should fail CreateContainer without leaking and notify the plugin with RemoveContainer "+
				"when it rejects CreateContainer",
			func(ctx SpecContext) {
				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-ctr", idx: "00"},
				)
				plugin := stubs[0].Plugin

				podID, podConfig := runRollbackContainerPod(ctx, rc, ic, "nri-test-rollback-ctr-")
				registerPod(podID)

				var reject atomic.Bool
				reject.Store(true)

				plugin.OnCreateContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					if reject.Load() {
						return errors.New("induced NRI CreateContainer rejection")
					}

					return nil
				}

				By("attempting CreateContainer")

				containerConfig, containerName := newRollbackContainerConfig(
					"nri-test-rollback-ctr-",
				)
				failedID, createErr := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				events := plugin.Events()

				Expect(createErr).To(HaveOccurred(),
					"CreateContainer MUST fail when the NRI CreateContainer hook returns an error")
				Expect(failedID).To(BeEmpty(),
					"no container ID should be returned when CreateContainer fails")

				createEvent := rollbackLastEvent(events, EventCreateContainer)
				Expect(createEvent).NotTo(BeNil(),
					"the plugin should have received CreateContainer before rejecting it")

				attemptedID := createEvent.ContainerID
				Expect(attemptedID).NotTo(BeEmpty())

				By("verifying the failed container is not listed when the call returns")
				expectRollbackContainerNotListed(ctx, rc, podID, attemptedID, containerName)

				By("verifying the plugin got RemoveContainer before the call returned")
				expectRollbackContainerTeardown(events, "the rejecting plugin", podID, attemptedID)

				By("retrying CreateContainer with the same metadata")

				reject.Store(false)
				retryRollbackContainer(ctx, rc, ic, containerConfig, podID, podConfig)
			},
		)

		It(
			"should fail CreateContainer without leaking and notify both plugins "+
				"when a higher-index plugin rejects it",
			func(ctx SpecContext) {
				By("starting an accepting plugin at index 00 and a rejecting plugin at index 10")

				stubs = startRollbackStubs(
					rollbackStubSpec{name: "cri-test-nri-rollback-ctr-accept", idx: "00"},
					rollbackStubSpec{name: "cri-test-nri-rollback-ctr-reject", idx: "10"},
				)
				accepting, rejecting := stubs[0].Plugin, stubs[1].Plugin

				podID, podConfig := runRollbackContainerPod(
					ctx,
					rc,
					ic,
					"nri-test-rollback-ctr-multi-",
				)
				registerPod(podID)

				var reject atomic.Bool
				reject.Store(true)

				rejecting.OnCreateContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					if reject.Load() {
						return errors.New("induced NRI CreateContainer rejection")
					}

					return nil
				}

				By("attempting CreateContainer")

				containerConfig, containerName := newRollbackContainerConfig(
					"nri-test-rollback-ctr-multi-",
				)
				failedID, createErr := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				acceptEvents, rejectEvents := accepting.Events(), rejecting.Events()

				Expect(createErr).To(HaveOccurred(),
					"CreateContainer MUST fail when an NRI plugin rejects the CreateContainer event")
				Expect(failedID).To(BeEmpty(),
					"no container ID should be returned when CreateContainer fails")

				acceptCreate := rollbackLastEvent(acceptEvents, EventCreateContainer)
				Expect(acceptCreate).NotTo(BeNil(),
					"the accepting plugin should have received CreateContainer before the rejection")

				attemptedID := acceptCreate.ContainerID
				Expect(attemptedID).NotTo(BeEmpty())

				By("verifying both plugins saw the same container in plugin index order")

				rejectCreate := rollbackFindEvent(
					rejectEvents,
					EventCreateContainer,
					podID,
					attemptedID,
				)
				Expect(rejectCreate).NotTo(BeNil(),
					"the rejecting plugin should have received CreateContainer for the same container")
				Expect(acceptCreate.Timestamp).NotTo(BeTemporally(">", rejectCreate.Timestamp),
					"CreateContainer hooks MUST execute in plugin index order (index 00 before index 10)")

				By("verifying the failed container is not listed when the call returns")
				expectRollbackContainerNotListed(ctx, rc, podID, attemptedID, containerName)

				By("verifying both plugins got RemoveContainer before the call returned")
				expectRollbackContainerTeardown(
					acceptEvents,
					"the accepting plugin at index 00",
					podID,
					attemptedID,
				)
				expectRollbackContainerTeardown(
					rejectEvents,
					"the rejecting plugin at index 10",
					podID,
					attemptedID,
				)

				By("retrying CreateContainer with the same metadata")

				reject.Store(false)
				retryRollbackContainer(ctx, rc, ic, containerConfig, podID, podConfig)
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

// runRollbackContainerPod pulls the test image and runs a pod sandbox for a
// CreateContainer rollback spec.
func runRollbackContainerPod(
	ctx context.Context,
	rc internalapi.RuntimeService,
	ic internalapi.ImageManagerService,
	prefix string,
) (podID string, podConfig *runtimeapi.PodSandboxConfig) {
	framework.PullPublicImage(
		ctx,
		ic,
		framework.TestContext.TestImageList.DefaultTestContainerImage,
		nil,
	)

	By("creating a pod sandbox")

	podConfig, _ = newRollbackPodConfig(ctx, rc, prefix)

	return framework.RunPodSandbox(ctx, rc, podConfig), podConfig
}

// newRollbackContainerConfig builds a uniquely named container config and
// returns it together with the container name.
func newRollbackContainerConfig(prefix string) (config *runtimeapi.ContainerConfig, name string) {
	name = prefix + framework.NewUUID()

	return &runtimeapi.ContainerConfig{
		Metadata: framework.BuildContainerMetadata(name, framework.DefaultAttempt),
		Image: &runtimeapi.ImageSpec{
			Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
		},
		Command: framework.DefaultPauseCommand,
		Linux:   &runtimeapi.LinuxContainerConfig{},
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
			"sandbox %s MUST be cleaned up by the time a failed RunPodSandbox returns", pod.GetId())
	}
}

// expectRollbackContainerNotListed asserts that no container with the given
// ID or name is listed in the sandbox.
func expectRollbackContainerNotListed(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podID, containerID, containerName string,
) {
	containers, err := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{PodSandboxId: podID})
	Expect(err).NotTo(HaveOccurred())

	for _, c := range containers {
		matches := c.GetId() == containerID ||
			(c.GetMetadata() != nil && c.GetMetadata().GetName() == containerName)
		Expect(matches).To(BeFalse(),
			"container %s MUST be cleaned up by the time a failed CreateContainer returns", c.GetId())
	}
}

// retryRollbackPod runs a sandbox with the config of a rolled-back attempt,
// registers it for cleanup, and stops and removes it.
func retryRollbackPod(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podConfig *runtimeapi.PodSandboxConfig,
	register func(string),
) {
	retryID, err := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
	register(retryID)
	Expect(err).NotTo(HaveOccurred(),
		"RunPodSandbox retried with the same metadata MUST succeed: "+rollbackRetryRationale)

	Expect(rc.StopPodSandbox(ctx, retryID)).To(Succeed(),
		"the retried sandbox %s MUST stop", retryID)
	Expect(rc.RemovePodSandbox(ctx, retryID)).To(Succeed(),
		"the retried sandbox %s MUST be removable", retryID)
}

// retryRollbackContainer creates a container with the config of a rolled-back
// attempt and removes it.
func retryRollbackContainer(
	ctx context.Context,
	rc internalapi.RuntimeService,
	ic internalapi.ImageManagerService,
	containerConfig *runtimeapi.ContainerConfig,
	podID string,
	podConfig *runtimeapi.PodSandboxConfig,
) {
	retryID, err := framework.CreateContainerWithError(
		ctx,
		rc,
		ic,
		containerConfig,
		podID,
		podConfig,
	)
	Expect(err).NotTo(HaveOccurred(),
		"CreateContainer retried with the same metadata MUST succeed: "+rollbackRetryRationale)

	Expect(rc.RemoveContainer(ctx, retryID)).To(Succeed(),
		"the retried container %s MUST be removable", retryID)
}

// expectRollbackPodTeardown asserts that the events, snapshotted when the
// failed RunPodSandbox returned, hold RunPodSandbox, then StopPodSandbox, then
// RemovePodSandbox for the sandbox.
func expectRollbackPodTeardown(events []NRIEvent, who, podID string) {
	expectRollbackTeardown(events, who, podID, "",
		EventRunPodSandbox, EventStopPodSandbox, EventRemovePodSandbox, true)
}

// expectRollbackContainerTeardown asserts that the events, snapshotted when the
// failed CreateContainer returned, hold CreateContainer, then RemoveContainer
// for the container, with StopContainer allowed in between.
func expectRollbackContainerTeardown(events []NRIEvent, who, podID, containerID string) {
	expectRollbackTeardown(events, who, podID, containerID,
		EventCreateContainer, EventStopContainer, EventRemoveContainer, false)
}

// expectRollbackTeardown asserts the rollback of one object as seen by one
// plugin: the create event comes first, every teardown event for the object
// comes after it, stop arrives at least once when stopRequired, and remove
// arrives at least once after any stop. Repeated teardown events are allowed.
func expectRollbackTeardown(
	events []NRIEvent,
	who, podID, containerID string,
	create, stop, remove NRIEventType,
	stopRequired bool,
) {
	observed := rollbackEventTypes(events, podID, containerID)
	createIdx, firstStop, lastRemove := -1, -1, -1

	for i := range events {
		e := &events[i]
		if !rollbackMatches(e, podID, containerID) {
			continue
		}

		if e.Type == create {
			if createIdx < 0 {
				createIdx = i
			}

			continue
		}

		if e.Type != stop && e.Type != remove {
			continue
		}

		Expect(createIdx).To(BeNumerically(">=", 0),
			"%s received %s before %s (events: %s); teardown MUST follow the create event",
			who, e.Type, create, observed)

		if e.Type == stop && firstStop < 0 {
			firstStop = i
		}

		if e.Type == remove {
			lastRemove = i
		}
	}

	Expect(createIdx).To(BeNumerically(">=", 0),
		"%s should have received %s (events: %s)", who, create, observed)

	if stopRequired {
		Expect(firstStop).To(BeNumerically(">=", 0),
			"%s MUST receive %s for the rolled-back object before the failed call returns "+
				"(events: %s): %s", who, stop, observed, rollbackRationale)
	}

	Expect(lastRemove).To(BeNumerically(">", max(createIdx, firstStop)),
		"%s MUST receive %s after %s%s for the rolled-back object before the failed call returns "+
			"(events: %s): %s", who, remove, create, rollbackStopNote(stop, firstStop), observed,
		rollbackRationale)
}

// rollbackStopNote names the stop event in a failure message when it was seen.
func rollbackStopNote(stop NRIEventType, firstStop int) string {
	if firstStop < 0 {
		return ""
	}

	return " and " + string(stop)
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

// rollbackEventTypes renders all event types recorded for the sandbox (and
// container, if non-empty), for use in failure messages.
func rollbackEventTypes(events []NRIEvent, podID, containerID string) string {
	var types []NRIEventType

	for i := range events {
		if rollbackMatches(&events[i], podID, containerID) {
			types = append(types, events[i].Type)
		}
	}

	return fmt.Sprint(types)
}
