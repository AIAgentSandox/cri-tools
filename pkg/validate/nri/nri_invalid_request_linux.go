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
	"time"

	nri "github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/framework"
)

// invalidRequestUnknownID is an object ID that no runtime will have allocated.
const invalidRequestUnknownID = "nri-test-invalid-request-unknown-id"

// The Update event types are recorded only by invalidRequestObserver. They are
// variables, not constants, so that the exhaustive switches over the shared
// lifecycle event types elsewhere in this package need no new cases.
var (
	invalidRequestEventUpdateContainer  = NRIEventType("UpdateContainer")
	invalidRequestEventUpdatePodSandbox = NRIEventType("UpdatePodSandbox")
)

// invalidRequestObserver records every lifecycle hook the shared test plugin
// records, plus the UpdateContainer and UpdatePodSandbox hooks, so that a
// window asserting that no Update hook arrived is observed by a plugin that
// subscribes to them.
type invalidRequestObserver struct {
	*NRITestPlugin
}

// UpdateContainer implements stub.UpdateContainerInterface.
func (o *invalidRequestObserver) UpdateContainer(
	_ context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
	_ *nri.LinuxResources,
) ([]*nri.ContainerUpdate, error) {
	o.recordContainerEvent(invalidRequestEventUpdateContainer, pod, container)

	return nil, nil
}

// UpdatePodSandbox implements stub.UpdatePodInterface.
func (o *invalidRequestObserver) UpdatePodSandbox(
	_ context.Context,
	pod *nri.PodSandbox,
	_, _ *nri.LinuxResources,
) error {
	o.recordPodEvent(invalidRequestEventUpdatePodSandbox, pod)

	return nil
}

// startInvalidRequestObserver connects an invalidRequestObserver to the
// runtime and waits until it is synchronized.
func startInvalidRequestObserver(pluginName, pluginIdx string) (*NRITestStub, error) {
	observer := &invalidRequestObserver{
		NRITestPlugin: &NRITestPlugin{ready: make(chan struct{})},
	}

	s, err := stub.New(observer,
		stub.WithPluginName(pluginName),
		stub.WithPluginIdx(pluginIdx),
		stub.WithSocketPath(framework.TestContext.NRISocketPath),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NRI stub: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(done)

		errCh <- s.Run(ctx)
	}()

	select {
	case <-observer.ready:
	case <-done:
		cancel()

		return nil, fmt.Errorf("NRI stub exited early: %w", <-errCh)
	case <-time.After(10 * time.Second):
		cancel()

		return nil, errors.New("NRI stub did not become ready within 10s")
	}

	return &NRITestStub{
		Plugin: observer.NRITestPlugin,
		Stub:   s,
		cancel: cancel,
		done:   done,
	}, nil
}

// expectNoNRIEventsSince asserts that no NRI event beyond the first
// baseline events is recorded by the plugin over a short time window.
//
// A request the runtime rejects, or that names an object the runtime does not
// have, must not deliver any hook: a plugin must never see an object that the
// runtime then refuses to create, or be told about one that does not exist.
func expectNoNRIEventsSince(plugin *NRITestPlugin, baseline int, description string) {
	GinkgoHelper()

	Consistently(func() []NRIEvent {
		return plugin.Events()[baseline:]
	}, 2*time.Second, 200*time.Millisecond).Should(BeEmpty(),
		"NRI hooks MUST NOT fire for %s", description)
}

// expectNRIEventSince is the positive control for expectNoNRIEventsSince: it
// asserts that the plugin records an eventType hook for objectID (a container
// ID for container hooks, a pod sandbox ID for pod hooks) after the first
// baseline events, which shows that the observer would have seen such a hook
// during the preceding window.
func expectNRIEventSince(
	plugin *NRITestPlugin,
	baseline int,
	eventType NRIEventType,
	objectID string,
) {
	GinkgoHelper()

	Eventually(func() bool {
		events := plugin.Events()
		for i := baseline; i < len(events); i++ {
			e := &events[i]
			if e.Type == eventType && (e.ContainerID == objectID || e.PodSandboxID == objectID) {
				return true
			}
		}

		return false
	}, 5*time.Second, 100*time.Millisecond).Should(BeTrue(),
		"positive control: the NRI plugin did not record %s for %q", eventType, objectID)
}

// invalidRequestContainerConfig returns a valid container config with a
// unique name, used as the base for the invalid CreateContainer requests.
func invalidRequestContainerConfig() *runtimeapi.ContainerConfig {
	return &runtimeapi.ContainerConfig{
		Metadata: framework.BuildContainerMetadata(
			"nri-test-invalid-req-ctr-"+framework.NewUUID(),
			framework.DefaultAttempt,
		),
		Image: &runtimeapi.ImageSpec{
			Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
		},
		Command: framework.DefaultPauseCommand,
		Linux:   &runtimeapi.LinuxContainerConfig{},
	}
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

	Context("invalid CRI requests", Serial, func() {
		var (
			testStub *NRITestStub
			podIDs   []string
		)

		BeforeEach(func() {
			podIDs = nil

			var err error

			testStub, err = startInvalidRequestObserver("cri-test-nri-invalid-req", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")
		})

		AfterEach(func(ctx SpecContext) {
			if testStub != nil {
				testStub.Cleanup()
				testStub = nil
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
			"should not invoke NRI hooks for CreateContainer in a non-existing sandbox",
			func(ctx SpecContext) {
				// A pod config is still required by the CRI call; it describes a
				// sandbox that was never created.
				podConfig := &runtimeapi.PodSandboxConfig{
					Metadata: framework.BuildPodSandboxMetadata(
						"nri-test-invalid-req-missing-pod-"+framework.NewUUID(),
						framework.DefaultUIDPrefix+framework.NewUUID(),
						framework.DefaultNamespacePrefix+framework.NewUUID(),
						framework.DefaultAttempt,
					),
					Labels: framework.DefaultPodLabels,
				}

				baseline := len(testStub.Plugin.Events())

				By("creating a container in a non-existing sandbox")

				_, err := framework.CreateContainerWithError(
					ctx,
					rc,
					ic,
					invalidRequestContainerConfig(),
					invalidRequestUnknownID,
					podConfig,
				)
				Expect(err).To(HaveOccurred(),
					"CreateContainer for a non-existing sandbox MUST fail")

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"CreateContainer in a non-existing sandbox")

				By("creating a container in a real sandbox as the positive control")

				podID, realPodConfig := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				ctrID := framework.CreateContainer(
					ctx, rc, ic, invalidRequestContainerConfig(), podID, realPodConfig,
				)
				expectNRIEventSince(testStub.Plugin, baseline, EventCreateContainer, ctrID)
			},
		)

		It(
			"should not invoke NRI hooks for CreateContainer without metadata",
			func(ctx SpecContext) {
				By("creating a pod sandbox")

				podID, podConfig := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				baseline := len(testStub.Plugin.Events())

				By("creating a container without metadata")

				containerConfig := invalidRequestContainerConfig()
				containerConfig.Metadata = nil

				ctrID, err := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(err).To(HaveOccurred(),
					"CreateContainer without container metadata MUST fail, got container %q", ctrID)

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"CreateContainer without metadata")

				By("creating a container with metadata as the positive control")

				ctrID = framework.CreateContainer(
					ctx, rc, ic, invalidRequestContainerConfig(), podID, podConfig,
				)
				expectNRIEventSince(testStub.Plugin, baseline, EventCreateContainer, ctrID)
			},
		)

		It("should not invoke NRI hooks for RunPodSandbox without metadata", func(ctx SpecContext) {
			baseline := len(testStub.Plugin.Events())

			By("running a pod sandbox without metadata")

			// The runtime is called directly instead of through
			// framework.RunPodSandboxError so that a sandbox ID returned by a
			// runtime which wrongly accepts the request is still recorded for
			// cleanup before the assertion fails the spec.
			podID, err := rc.RunPodSandbox(ctx, &runtimeapi.PodSandboxConfig{
				Labels: framework.DefaultPodLabels,
			}, framework.TestContext.RuntimeHandler)
			if podID != "" {
				podIDs = append(podIDs, podID)
			}

			Expect(err).To(HaveOccurred(), "RunPodSandbox without metadata MUST fail")

			expectNoNRIEventsSince(testStub.Plugin, baseline,
				"RunPodSandbox without metadata")

			By("running a pod sandbox with metadata as the positive control")

			podID, _ = framework.CreatePodSandboxForContainer(ctx, rc)
			podIDs = append(podIDs, podID)
			expectNRIEventSince(testStub.Plugin, baseline, EventRunPodSandbox, podID)
		})

		It(
			"should not invoke NRI hooks for RunPodSandbox duplicating an existing sandbox's metadata",
			func(ctx SpecContext) {
				By("creating a pod sandbox")

				podID, podConfig := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				baseline := len(testStub.Plugin.Events())

				By("running a second pod sandbox with the same name, uid, namespace and attempt")

				dupID, err := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
				if dupID != "" && dupID != podID {
					podIDs = append(podIDs, dupID)
				}

				// The CRI metadata uniquely identifies a sandbox, so the runtime MUST
				// NOT create a second sandbox for the duplicate request. Two outcomes
				// are allowed: the request fails (containerd rejects it because the
				// sandbox name is already reserved), or it returns the existing
				// sandbox ID so that a kubelet retry is idempotent. Creating another
				// sandbox is not allowed, and no NRI hook may run in either case.
				if err == nil {
					Expect(dupID).To(Equal(podID),
						"RunPodSandbox with metadata (name/uid/namespace/attempt) identical to the "+
							"existing sandbox %q MUST either fail or return that same sandbox, "+
							"got new sandbox %q", podID, dupID)
					framework.Logf(
						"RunPodSandbox with duplicated metadata returned the existing sandbox %q",
						podID,
					)
				} else {
					framework.Logf("RunPodSandbox with duplicated metadata was rejected: %v", err)
				}

				By("listing the sandboxes with the duplicated metadata")

				sandboxes, err := rc.ListPodSandbox(ctx, nil)
				Expect(err).NotTo(HaveOccurred(), "failed to list pod sandboxes")

				var matching []string

				want := podConfig.GetMetadata()
				for _, sb := range sandboxes {
					got := sb.GetMetadata()
					if got.GetName() == want.GetName() && got.GetUid() == want.GetUid() &&
						got.GetNamespace() == want.GetNamespace() &&
						got.GetAttempt() == want.GetAttempt() {
						matching = append(matching, sb.GetId())
					}
				}

				Expect(matching).To(ConsistOf(podID),
					"RunPodSandbox duplicating the metadata of sandbox %q MUST NOT create a "+
						"second sandbox with that metadata", podID)

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"RunPodSandbox duplicating an existing sandbox's metadata")

				By("running a pod sandbox with new metadata as the positive control")

				otherID, _ := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, otherID)
				expectNRIEventSince(testStub.Plugin, baseline, EventRunPodSandbox, otherID)
			},
		)

		It(
			"should not invoke NRI hooks for container operations on an unknown container ID",
			func(ctx SpecContext) {
				baseline := len(testStub.Plugin.Events())

				By("starting an unknown container")
				Expect(rc.StartContainer(ctx, invalidRequestUnknownID)).To(HaveOccurred(),
					"StartContainer for an unknown container MUST fail")

				// The CRI allows StopContainer and RemoveContainer to succeed for a
				// container that does not exist, so only the absence of NRI hooks is
				// asserted for them.
				By("stopping an unknown container")

				if err := rc.StopContainer(ctx, invalidRequestUnknownID, 0); err != nil {
					framework.Logf("StopContainer(%s) returned: %v", invalidRequestUnknownID, err)
				}

				By("removing an unknown container")

				if err := rc.RemoveContainer(ctx, invalidRequestUnknownID); err != nil {
					framework.Logf("RemoveContainer(%s) returned: %v", invalidRequestUnknownID, err)
				}

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"StartContainer, StopContainer or RemoveContainer on an unknown container")

				By("starting, stopping and removing a real container as the positive control")

				podID, podConfig := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				ctrID := framework.CreateContainer(
					ctx, rc, ic, invalidRequestContainerConfig(), podID, podConfig,
				)

				Expect(rc.StartContainer(ctx, ctrID)).To(Succeed(), "failed to start container")
				expectNRIEventSince(testStub.Plugin, baseline, EventStartContainer, ctrID)

				Expect(rc.StopContainer(ctx, ctrID, 0)).To(Succeed(), "failed to stop container")
				expectNRIEventSince(testStub.Plugin, baseline, EventStopContainer, ctrID)

				Expect(rc.RemoveContainer(ctx, ctrID)).To(Succeed(), "failed to remove container")
				expectNRIEventSince(testStub.Plugin, baseline, EventRemoveContainer, ctrID)
			},
		)

		It(
			"should not invoke NRI hooks for pod sandbox operations on an unknown sandbox ID",
			func(ctx SpecContext) {
				baseline := len(testStub.Plugin.Events())

				// The CRI allows StopPodSandbox and RemovePodSandbox to succeed for a
				// sandbox that does not exist, so only the absence of NRI hooks is
				// asserted.
				By("stopping an unknown pod sandbox")

				if err := rc.StopPodSandbox(ctx, invalidRequestUnknownID); err != nil {
					framework.Logf("StopPodSandbox(%s) returned: %v", invalidRequestUnknownID, err)
				}

				By("removing an unknown pod sandbox")

				if err := rc.RemovePodSandbox(ctx, invalidRequestUnknownID); err != nil {
					framework.Logf(
						"RemovePodSandbox(%s) returned: %v",
						invalidRequestUnknownID,
						err,
					)
				}

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"StopPodSandbox or RemovePodSandbox on an unknown sandbox")

				By("stopping and removing a real pod sandbox as the positive control")

				podID, _ := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				Expect(rc.StopPodSandbox(ctx, podID)).To(Succeed(), "failed to stop pod sandbox")
				expectNRIEventSince(testStub.Plugin, baseline, EventStopPodSandbox, podID)

				Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed(),
					"failed to remove pod sandbox")
				expectNRIEventSince(testStub.Plugin, baseline, EventRemovePodSandbox, podID)
			},
		)

		It(
			"should not invoke NRI hooks for resource updates on an unknown ID",
			func(ctx SpecContext) {
				baseline := len(testStub.Plugin.Events())

				resources := &runtimeapi.LinuxContainerResources{
					CpuShares:          512,
					MemoryLimitInBytes: 64 * 1024 * 1024,
				}

				// The outcome of the CRI call is not asserted: only the absence of
				// NRI hooks for an object the runtime does not have.
				By("updating the resources of an unknown container")

				err := rc.UpdateContainerResources(ctx, invalidRequestUnknownID,
					&runtimeapi.ContainerResources{Linux: resources})
				framework.Logf(
					"UpdateContainerResources(%s) returned: %v",
					invalidRequestUnknownID,
					err,
				)

				By("updating the resources of an unknown pod sandbox")

				_, podErr := rc.UpdatePodSandboxResources(ctx,
					&runtimeapi.UpdatePodSandboxResourcesRequest{
						PodSandboxId: invalidRequestUnknownID,
						Resources:    resources,
					})
				framework.Logf(
					"UpdatePodSandboxResources(%s) returned: %v",
					invalidRequestUnknownID,
					podErr,
				)

				expectNoNRIEventsSince(testStub.Plugin, baseline,
					"UpdateContainerResources or UpdatePodSandboxResources on an unknown ID")

				By("updating the resources of a real container as the positive control")

				podID, podConfig := framework.CreatePodSandboxForContainer(ctx, rc)
				podIDs = append(podIDs, podID)

				ctrID := framework.CreateContainer(
					ctx, rc, ic, invalidRequestContainerConfig(), podID, podConfig,
				)
				Expect(rc.StartContainer(ctx, ctrID)).To(Succeed(), "failed to start container")

				Expect(rc.UpdateContainerResources(ctx, ctrID,
					&runtimeapi.ContainerResources{Linux: resources})).To(Succeed(),
					"failed to update container resources")
				expectNRIEventSince(
					testStub.Plugin,
					baseline,
					invalidRequestEventUpdateContainer,
					ctrID,
				)

				By("updating the resources of a real pod sandbox as the positive control")

				_, err = rc.UpdatePodSandboxResources(ctx,
					&runtimeapi.UpdatePodSandboxResourcesRequest{
						PodSandboxId: podID,
						Resources:    resources,
					})
				if status.Code(err) == codes.Unimplemented {
					Skip(
						"UpdatePodSandboxResources is not implemented by the runtime: " + err.Error(),
					)
				}

				Expect(err).NotTo(HaveOccurred(), "failed to update pod sandbox resources")
				expectNRIEventSince(
					testStub.Plugin,
					baseline,
					invalidRequestEventUpdatePodSandbox,
					podID,
				)
			},
		)
	})
})
