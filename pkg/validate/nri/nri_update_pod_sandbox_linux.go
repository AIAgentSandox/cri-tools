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
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

// updatePodSandboxCall records a single NRI UpdatePodSandbox request.
type updatePodSandboxCall struct {
	podID     string
	podName   string
	podUID    string
	overhead  *nri.LinuxResources
	resources *nri.LinuxResources
}

// updatePodSandboxPlugin is an NRI plugin dedicated to the UpdatePodSandbox
// tests. It records UpdatePodSandbox requests and PostUpdatePodSandbox events,
// and can be configured to reject UpdatePodSandbox requests with an error.
type updatePodSandboxPlugin struct {
	mu          sync.Mutex
	updates     []updatePodSandboxCall
	postUpdates []string
	updateErr   error

	ready     chan struct{}
	readyOnce sync.Once
}

// Synchronize implements stub.SynchronizeInterface and signals readiness.
func (p *updatePodSandboxPlugin) Synchronize(
	context.Context,
	[]*nri.PodSandbox,
	[]*nri.Container,
) ([]*nri.ContainerUpdate, error) {
	p.readyOnce.Do(func() { close(p.ready) })

	return nil, nil
}

// UpdatePodSandbox implements stub.UpdatePodInterface.
func (p *updatePodSandboxPlugin) UpdatePodSandbox(
	_ context.Context,
	pod *nri.PodSandbox,
	overhead, resources *nri.LinuxResources,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updates = append(p.updates, updatePodSandboxCall{
		podID:     pod.GetId(),
		podName:   pod.GetName(),
		podUID:    pod.GetUid(),
		overhead:  overhead,
		resources: resources,
	})

	return p.updateErr
}

// PostUpdatePodSandbox implements stub.PostUpdatePodInterface.
func (p *updatePodSandboxPlugin) PostUpdatePodSandbox(
	_ context.Context,
	pod *nri.PodSandbox,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.postUpdates = append(p.postUpdates, pod.GetId())

	return nil
}

// setUpdateErr configures the error returned from subsequent UpdatePodSandbox requests.
func (p *updatePodSandboxPlugin) setUpdateErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateErr = err
}

// updatesFor returns the UpdatePodSandbox requests recorded for podID.
func (p *updatePodSandboxPlugin) updatesFor(podID string) []updatePodSandboxCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result []updatePodSandboxCall

	for i := range p.updates {
		if p.updates[i].podID == podID {
			result = append(result, p.updates[i])
		}
	}

	return result
}

// postUpdateCountFor returns the number of PostUpdatePodSandbox events recorded for podID.
func (p *updatePodSandboxPlugin) postUpdateCountFor(podID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0

	for _, id := range p.postUpdates {
		if id == podID {
			count++
		}
	}

	return count
}

// updatePodSandboxStub runs an updatePodSandboxPlugin connected to the runtime.
type updatePodSandboxStub struct {
	plugin *updatePodSandboxPlugin
	cancel context.CancelFunc
	done   chan struct{}
}

// startUpdatePodSandboxStub connects an updatePodSandboxPlugin to the
// runtime's NRI socket and waits for the registration handshake to complete.
func startUpdatePodSandboxStub(pluginName, pluginIdx string) (*updatePodSandboxStub, error) {
	socketPath := framework.TestContext.NRISocketPath
	if socketPath == "" {
		return nil, errors.New("NRI socket path not configured")
	}

	plugin := &updatePodSandboxPlugin{ready: make(chan struct{})}

	s, err := stub.New(plugin,
		stub.WithPluginName(pluginName),
		stub.WithPluginIdx(pluginIdx),
		stub.WithSocketPath(socketPath),
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

	ts := &updatePodSandboxStub{plugin: plugin, cancel: cancel, done: done}

	select {
	case <-done:
		cancel()

		return nil, fmt.Errorf("NRI stub exited early: %w", <-errCh)
	case <-plugin.ready:
		return ts, nil
	case <-time.After(10 * time.Second):
		ts.stop()

		return nil, errors.New("NRI stub did not become ready within 10s")
	}
}

// stop disconnects the plugin from the runtime.
func (ts *updatePodSandboxStub) stop() {
	ts.cancel()

	select {
	case <-ts.done:
	case <-time.After(5 * time.Second):
		framework.Logf("NRI UpdatePodSandbox stub did not stop within 5s")
	}
}

// skipIfUpdatePodSandboxResourcesUnimplemented probes the CRI
// UpdatePodSandboxResources API with a nonexistent sandbox ID and skips the
// spec if the runtime does not implement it. The probe must run before the
// plugin connects: runtimes predating UpdatePodSandboxResources (e.g.
// containerd 2.0) also predate the NRI UpdatePodSandbox events and reject a
// plugin subscribing to them during registration.
func skipIfUpdatePodSandboxResourcesUnimplemented(
	ctx context.Context,
	rc internalapi.RuntimeService,
) {
	_, err := rc.UpdatePodSandboxResources(ctx, &runtimeapi.UpdatePodSandboxResourcesRequest{
		PodSandboxId: "nri-update-pod-probe-" + framework.NewUUID(),
	})
	if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
		Skip("runtime does not implement CRI UpdatePodSandboxResources " +
			"(added in containerd 2.1); NRI UpdatePodSandbox cannot be triggered: " + s.Message())
	}
}

// expectNRIResourcesMatch asserts that the NRI resources relayed to the
// plugin carry the values requested via CRI.
func expectNRIResourcesMatch(
	got *nri.LinuxResources,
	want *runtimeapi.LinuxContainerResources,
	what string,
) {
	Expect(got).NotTo(BeNil(), "NRI %s resources should be set", what)
	Expect(got.GetCpu().GetShares().GetValue()).To(BeEquivalentTo(want.GetCpuShares()),
		"NRI %s CPU shares should match the CRI request", what)
	Expect(got.GetCpu().GetQuota().GetValue()).To(Equal(want.GetCpuQuota()),
		"NRI %s CPU quota should match the CRI request", what)
	Expect(got.GetCpu().GetPeriod().GetValue()).To(BeEquivalentTo(want.GetCpuPeriod()),
		"NRI %s CPU period should match the CRI request", what)
	Expect(got.GetMemory().GetLimit().GetValue()).To(Equal(want.GetMemoryLimitInBytes()),
		"NRI %s memory limit should match the CRI request", what)
}

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var rc internalapi.RuntimeService

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
	})

	Context("UpdatePodSandbox", Serial, func() {
		var (
			testStub *updatePodSandboxStub
			podID    string
		)

		const mib = 1024 * 1024

		newUpdateRequest := func(id string) *runtimeapi.UpdatePodSandboxResourcesRequest {
			return &runtimeapi.UpdatePodSandboxResourcesRequest{
				PodSandboxId: id,
				Overhead: &runtimeapi.LinuxContainerResources{
					CpuShares:          10,
					CpuQuota:           5000,
					CpuPeriod:          100000,
					MemoryLimitInBytes: 16 * mib,
				},
				Resources: &runtimeapi.LinuxContainerResources{
					CpuShares:          512,
					CpuQuota:           50000,
					CpuPeriod:          100000,
					MemoryLimitInBytes: 256 * mib,
				},
			}
		}

		runPod := func(ctx context.Context, prefix string) (string, *runtimeapi.PodSandboxConfig) {
			podConfig := &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(
					prefix+framework.NewUUID(),
					framework.DefaultUIDPrefix+framework.NewUUID(),
					framework.DefaultNamespacePrefix+framework.NewUUID(),
					framework.DefaultAttempt,
				),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}

			id := framework.RunPodSandbox(ctx, rc, podConfig)
			Expect(id).NotTo(BeEmpty())

			return id, podConfig
		}

		updateResources := func(
			ctx context.Context,
			req *runtimeapi.UpdatePodSandboxResourcesRequest,
		) error {
			_, err := rc.UpdatePodSandboxResources(ctx, req)

			return err
		}

		BeforeEach(func(ctx SpecContext) {
			skipIfUpdatePodSandboxResourcesUnimplemented(ctx, rc)
		})

		AfterEach(func(ctx SpecContext) {
			if testStub != nil {
				testStub.stop()
				testStub = nil
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
			"should relay CRI UpdatePodSandboxResources to NRI UpdatePodSandbox with the pod and requested resources",
			func(ctx SpecContext) {
				var err error

				testStub, err = startUpdatePodSandboxStub("cri-test-nri-update-pod", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				By("creating a pod sandbox")

				var podConfig *runtimeapi.PodSandboxConfig

				podID, podConfig = runPod(ctx, "nri-test-update-pod-")

				By("calling CRI UpdatePodSandboxResources")

				req := newUpdateRequest(podID)
				Expect(updateResources(ctx, req)).To(Succeed(),
					"UpdatePodSandboxResources should succeed when the NRI plugin accepts the update")

				By(
					"verifying the NRI UpdatePodSandbox request carries the pod and requested resources",
				)

				updates := testStub.plugin.updatesFor(podID)
				// UpdatePodSandbox is a synchronous NRI request, so it must have
				// been delivered before the CRI call returned.
				Expect(updates).To(HaveLen(1),
					"NRI UpdatePodSandbox should be delivered exactly once before the CRI call returns")
				Expect(updates[0].podName).To(Equal(podConfig.GetMetadata().GetName()))
				Expect(updates[0].podUID).To(Equal(podConfig.GetMetadata().GetUid()))
				expectNRIResourcesMatch(updates[0].resources, req.GetResources(), "pod")
				expectNRIResourcesMatch(updates[0].overhead, req.GetOverhead(), "overhead")

				By("verifying the NRI PostUpdatePodSandbox event is delivered")
				Eventually(func() int {
					return testStub.plugin.postUpdateCountFor(podID)
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"NRI PostUpdatePodSandbox should be delivered once after a successful update")
			},
		)

		It("should fail CRI UpdatePodSandboxResources when the NRI plugin rejects UpdatePodSandbox",
			func(ctx SpecContext) {
				var err error

				testStub, err = startUpdatePodSandboxStub("cri-test-nri-update-pod-fail", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				const injected = "cri-test injected UpdatePodSandbox failure"

				testStub.plugin.setUpdateErr(errors.New(injected))

				By("creating a pod sandbox")

				podID, _ = runPod(ctx, "nri-test-update-pod-fail-")

				By("calling CRI UpdatePodSandboxResources with the plugin rejecting the update")

				updateErr := updateResources(ctx, newUpdateRequest(podID))
				Expect(updateErr).To(HaveOccurred(),
					"UpdatePodSandboxResources should fail when an NRI plugin rejects UpdatePodSandbox")
				Expect(updateErr.Error()).To(ContainSubstring(injected),
					"the CRI error should carry the NRI plugin's error message")
				Expect(testStub.plugin.updatesFor(podID)).To(HaveLen(1),
					"NRI UpdatePodSandbox should have been delivered to the plugin")

				By(
					"verifying no NRI PostUpdatePodSandbox event is delivered for the rejected update",
				)
				Consistently(func() int {
					return testStub.plugin.postUpdateCountFor(podID)
				}, 2*time.Second, 200*time.Millisecond).Should(Equal(0),
					"NRI PostUpdatePodSandbox must not be delivered when the update was rejected")

				By("verifying the pod sandbox is still ready")

				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred(), "PodSandboxStatus after rejected update")
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"a rejected resource update should not affect the sandbox state")

				By("verifying the plugin stays connected and a later update succeeds")
				testStub.plugin.setUpdateErr(nil)
				Expect(
					updateResources(ctx, newUpdateRequest(podID)),
				).To(Succeed(),
					"UpdatePodSandboxResources should succeed once the plugin accepts the update")
				Expect(testStub.plugin.updatesFor(podID)).To(HaveLen(2),
					"the retried update should be delivered to the same, still connected, plugin")
				Eventually(func() int {
					return testStub.plugin.postUpdateCountFor(podID)
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"NRI PostUpdatePodSandbox should be delivered once for the successful retry")
			})
	})
})
