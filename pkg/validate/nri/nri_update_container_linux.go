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
	"net"
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/stub"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

const (
	updateEventUpdate     = "UpdateContainer"
	updateEventPostUpdate = "PostUpdateContainer"

	updateInitialCPUShares   = int64(1024)
	updateInitialMemoryLimit = int64(256 * 1024 * 1024)
	updateRequestCPUShares   = int64(512)
	updateRequestMemoryLimit = int64(200 * 1024 * 1024)
	updatePluginMemoryLimit  = int64(160 * 1024 * 1024)
)

// updateEvent records an UpdateContainer or PostUpdateContainer NRI event.
type updateEvent struct {
	Type          string
	PodSandboxID  string
	PodName       string
	PodNamespace  string
	PodUID        string
	ContainerID   string
	ContainerName string
	// Resources holds the requested resources for UpdateContainer and the
	// container's resources as reported by the runtime for PostUpdateContainer.
	Resources *nri.LinuxResources
}

// updateTestPlugin is a dedicated NRI plugin that subscribes only to the
// UpdateContainer and PostUpdateContainer events.
type updateTestPlugin struct {
	mu     sync.Mutex
	events []updateEvent

	ready     chan struct{}
	readyOnce sync.Once

	// onUpdate, if set, is invoked from UpdateContainer and its result is
	// returned to the runtime.
	onUpdate func(container *nri.Container, req *nri.LinuxResources) ([]*nri.ContainerUpdate, error)

	// postUpdateErr, if set, is returned from PostUpdateContainer.
	postUpdateErr error
}

// Synchronize implements stub.SynchronizeInterface and signals readiness.
func (p *updateTestPlugin) Synchronize(
	_ context.Context,
	_ []*nri.PodSandbox,
	_ []*nri.Container,
) ([]*nri.ContainerUpdate, error) {
	p.readyOnce.Do(func() { close(p.ready) })

	return nil, nil
}

// UpdateContainer implements stub.UpdateContainerInterface.
func (p *updateTestPlugin) UpdateContainer(
	_ context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
	req *nri.LinuxResources,
) ([]*nri.ContainerUpdate, error) {
	p.record(updateEventUpdate, pod, container, req)

	p.mu.Lock()
	hook := p.onUpdate
	p.mu.Unlock()

	if hook != nil {
		return hook(container, req)
	}

	return nil, nil
}

// PostUpdateContainer implements stub.PostUpdateContainerInterface.
func (p *updateTestPlugin) PostUpdateContainer(
	_ context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
) error {
	p.record(updateEventPostUpdate, pod, container, container.GetLinux().GetResources())

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.postUpdateErr
}

func (p *updateTestPlugin) setOnUpdate(
	hook func(container *nri.Container, req *nri.LinuxResources) ([]*nri.ContainerUpdate, error),
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onUpdate = hook
}

func (p *updateTestPlugin) setPostUpdateErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.postUpdateErr = err
}

func (p *updateTestPlugin) record(
	eventType string,
	pod *nri.PodSandbox,
	container *nri.Container,
	resources *nri.LinuxResources,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var res *nri.LinuxResources
	if resources != nil {
		res = resources.Copy()
	}

	p.events = append(p.events, updateEvent{
		Type:          eventType,
		PodSandboxID:  pod.GetId(),
		PodName:       pod.GetName(),
		PodNamespace:  pod.GetNamespace(),
		PodUID:        pod.GetUid(),
		ContainerID:   container.GetId(),
		ContainerName: container.GetName(),
		Resources:     res,
	})
}

// eventsFor returns the recorded events of the given type for a container.
func (p *updateTestPlugin) eventsFor(eventType, containerID string) []updateEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result []updateEvent

	for _, e := range p.events {
		if e.Type == eventType && e.ContainerID == containerID {
			result = append(result, e)
		}
	}

	return result
}

// eventTypesFor returns the ordered event types recorded for a container.
func (p *updateTestPlugin) eventTypesFor(containerID string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result []string

	for _, e := range p.events {
		if e.ContainerID == containerID {
			result = append(result, e.Type)
		}
	}

	return result
}

// startUpdateTestPlugin connects a new updateTestPlugin to the runtime and
// waits for the registration handshake to complete. The returned stop
// function disconnects it.
func startUpdateTestPlugin() (*updateTestPlugin, func(), error) {
	socketPath := framework.TestContext.NRISocketPath
	if socketPath == "" {
		return nil, nil, errors.New("NRI socket path not configured")
	}

	plugin := &updateTestPlugin{ready: make(chan struct{})}

	// Keep the connection so stop can force-close it. stub.Stop takes the
	// stub's lock, which Start holds while waiting for the runtime and Run's
	// own Stop holds while waiting for in-flight handlers, so calling it from
	// here can block forever. Closing the connection unblocks both.
	var (
		connMu sync.Mutex
		conn   net.Conn
	)

	dialer := func(path string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return nil, err
		}

		connMu.Lock()
		conn = c
		connMu.Unlock()

		return c, nil
	}

	s, err := stub.New(plugin,
		stub.WithPluginName("cri-test-nri-update"),
		stub.WithPluginIdx("00"),
		stub.WithSocketPath(socketPath),
		stub.WithDialer(dialer),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create NRI stub: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(done)

		errCh <- s.Run(ctx)
	}()

	stop := func() {
		cancel()

		select {
		case <-done:
			return
		case <-time.After(5 * time.Second):
		}

		connMu.Lock()
		if conn != nil {
			conn.Close()
		}
		connMu.Unlock()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			framework.Logf(
				"NRI update test stub still running 10s after stop; leaking its goroutine",
			)
		}
	}

	select {
	case <-plugin.ready:
		return plugin, stop, nil
	case <-done:
		cancel()

		return nil, nil, fmt.Errorf("NRI stub exited early: %w", <-errCh)
	case <-time.After(10 * time.Second):
		stop()

		return nil, nil, errors.New("NRI stub did not become ready within 10s")
	}
}

// updateRequestResources returns the resources requested via CRI
// UpdateContainerResources in the tests.
func updateRequestResources() *runtimeapi.ContainerResources {
	return &runtimeapi.ContainerResources{
		Linux: &runtimeapi.LinuxContainerResources{
			CpuShares:          updateRequestCPUShares,
			MemoryLimitInBytes: updateRequestMemoryLimit,
		},
	}
}

// statusLinuxResources returns the Linux resources reported by ContainerStatus.
func statusLinuxResources(
	ctx context.Context,
	rc internalapi.RuntimeService,
	containerID string,
) *runtimeapi.LinuxContainerResources {
	resp, err := rc.ContainerStatus(ctx, containerID, false)
	Expect(err).NotTo(HaveOccurred(), "ContainerStatus(%s)", containerID)
	Expect(resp.GetStatus()).NotTo(BeNil(),
		"ContainerStatus(%s) MUST return a status for an existing container", containerID)

	return resp.GetStatus().GetResources().GetLinux()
}

// cpuWeight converts CPU shares to a cgroup v2 cpu.weight with the formula
// runc and crun use. A runtime on cgroup v2 may report shares recovered from
// the weight, which is lossy, so CPU shares are compared after this
// normalization rather than as raw values.
func cpuWeight(shares int64) int64 {
	if shares == 0 {
		return 0
	}

	return 1 + ((shares-2)*9999)/262142
}

// expectSameCPUShares asserts that two CPU share values are equal after
// cgroup v2 weight normalization.
func expectSameCPUShares(got, want int64, description string) {
	GinkgoHelper()
	Expect(cpuWeight(got)).To(Equal(cpuWeight(want)),
		"%s: got %d shares (weight %d), want %d shares (weight %d)",
		description, got, cpuWeight(got), want, cpuWeight(want))
}

// expectOnePostUpdate waits for a PostUpdateContainer event for the
// container and then checks that no second one follows.
func expectOnePostUpdate(plugin *updateTestPlugin, containerID string) {
	GinkgoHelper()

	count := func() int { return len(plugin.eventsFor(updateEventPostUpdate, containerID)) }

	Eventually(count, 10*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
		"PostUpdateContainer MUST be delivered")
	Consistently(count, 2*time.Second, 200*time.Millisecond).Should(Equal(1),
		"PostUpdateContainer MUST be delivered exactly once")
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

	Context("UpdateContainer", Serial, func() {
		var (
			plugin      *updateTestPlugin
			stopPlugin  func()
			podID       string
			podMeta     *runtimeapi.PodSandboxMetadata
			containerID string
			ctrName     string
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			plugin, stopPlugin, err = startUpdateTestPlugin()
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI update test plugin")

			framework.PullPublicImage(
				ctx,
				ic,
				framework.TestContext.TestImageList.DefaultTestContainerImage,
				nil,
			)

			By("creating a pod sandbox")

			podMeta = framework.BuildPodSandboxMetadata(
				"nri-test-update-"+framework.NewUUID(),
				framework.DefaultUIDPrefix+framework.NewUUID(),
				framework.DefaultNamespacePrefix+framework.NewUUID(),
				framework.DefaultAttempt,
			)
			podConfig := &runtimeapi.PodSandboxConfig{
				Metadata: podMeta,
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}
			podID = framework.RunPodSandbox(ctx, rc, podConfig)

			By("creating and starting a container with initial resources")

			ctrName = "nri-test-update-ctr-" + framework.NewUUID()
			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(ctrName, framework.DefaultAttempt),
				Image: &runtimeapi.ImageSpec{
					Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
				},
				Command: framework.DefaultPauseCommand,
				Linux: &runtimeapi.LinuxContainerConfig{
					Resources: &runtimeapi.LinuxContainerResources{
						CpuShares:          updateInitialCPUShares,
						MemoryLimitInBytes: updateInitialMemoryLimit,
					},
				},
			}
			containerID = framework.CreateContainer(ctx, rc, ic, containerConfig, podID, podConfig)
			Expect(rc.StartContainer(ctx, containerID)).To(Succeed())
		})

		AfterEach(func(ctx SpecContext) {
			// Disconnect the plugin first so a failing hook cannot interfere
			// with teardown.
			if stopPlugin != nil {
				stopPlugin()
			}

			if containerID != "" {
				if err := rc.StopContainer(ctx, containerID, 0); err != nil {
					framework.Logf("AfterEach: StopContainer(%s) failed: %v", containerID, err)
				}

				if err := rc.RemoveContainer(ctx, containerID); err != nil {
					framework.Logf("AfterEach: RemoveContainer(%s) failed: %v", containerID, err)
				}
			}

			if podID != "" {
				if err := rc.StopPodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", podID, err)
				}

				if err := rc.RemovePodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", podID, err)
				}
			}
		})

		// expectIdentity checks that an event carries the pod and container
		// identity and the metadata from the CRI requests. A plugin keys its
		// per-container state on these, so a wrong one makes it update the
		// wrong object.
		expectIdentity := func(e updateEvent) {
			GinkgoHelper()
			Expect(e.PodSandboxID).To(Equal(podID), "%s MUST carry the pod sandbox ID", e.Type)
			Expect(e.ContainerID).To(Equal(containerID), "%s MUST carry the container ID", e.Type)
			Expect(e.PodName).To(Equal(podMeta.GetName()), "%s MUST carry the pod name", e.Type)
			Expect(
				e.PodNamespace,
			).To(Equal(podMeta.GetNamespace()), "%s MUST carry the pod namespace", e.Type)
			Expect(e.PodUID).To(Equal(podMeta.GetUid()), "%s MUST carry the pod UID", e.Type)
			Expect(e.ContainerName).To(Equal(ctrName), "%s MUST carry the container name", e.Type)
		}

		// The rules below have no public specification: containerd/nri#286
		// (unmerged draft, revision 2339aa3) covers only pod sandbox hooks.
		// UpdateContainer lets plugins see and override a CRI resource
		// update before it is applied, and PostUpdateContainer is how a
		// plugin learns the values that were finally applied, as
		// PostCreateContainer is for a create.

		// A successful CRI UpdateContainerResources delivers UpdateContainer
		// once with the requested resources, then PostUpdateContainer once
		// with the applied resources.
		It(
			"should deliver UpdateContainer then PostUpdateContainer once each with the requested resources",
			func(ctx SpecContext) {
				By("updating the running container's resources via CRI")
				Expect(
					rc.UpdateContainerResources(ctx, containerID, updateRequestResources()),
				).To(Succeed())

				By("verifying each event is delivered exactly once")
				expectOnePostUpdate(plugin, containerID)
				Expect(plugin.eventsFor(updateEventUpdate, containerID)).To(HaveLen(1),
					"UpdateContainer MUST be delivered exactly once")

				By("verifying event order")
				Expect(plugin.eventTypesFor(containerID)).To(Equal(
					[]string{updateEventUpdate, updateEventPostUpdate}),
					"UpdateContainer MUST be delivered before PostUpdateContainer")

				By(
					"verifying the UpdateContainer event carries the identity and requested resources",
				)

				update := plugin.eventsFor(updateEventUpdate, containerID)[0]
				expectIdentity(update)
				Expect(update.Resources).NotTo(BeNil(),
					"UpdateContainer MUST carry the requested Linux resources")
				Expect(update.Resources.GetCpu().GetShares().GetValue()).To(
					BeEquivalentTo(updateRequestCPUShares),
					"UpdateContainer MUST carry the requested CPU shares")
				Expect(update.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updateRequestMemoryLimit),
					"UpdateContainer MUST carry the requested memory limit")

				By(
					"verifying the PostUpdateContainer event carries the identity and applied resources",
				)

				post := plugin.eventsFor(updateEventPostUpdate, containerID)[0]
				expectIdentity(post)
				Expect(post.Resources).NotTo(BeNil(),
					"PostUpdateContainer MUST carry the container's applied Linux resources")
				expectSameCPUShares(int64(post.Resources.GetCpu().GetShares().GetValue()),
					updateRequestCPUShares, "PostUpdateContainer MUST carry the applied CPU shares")
				Expect(post.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updateRequestMemoryLimit),
					"PostUpdateContainer MUST carry the applied memory limit")
			},
		)

		// A resource override returned from UpdateContainer wins over the
		// CRI request; fields the plugin does not set keep the requested
		// values.
		It(
			"should apply resources adjusted by the plugin in UpdateContainer",
			func(ctx SpecContext) {
				// Whether the runtime reports resources at all is read before
				// the update, independently of the outcome under test.
				before := statusLinuxResources(ctx, rc, containerID)

				plugin.setOnUpdate(
					func(container *nri.Container, _ *nri.LinuxResources) ([]*nri.ContainerUpdate, error) {
						u := &nri.ContainerUpdate{}
						u.SetContainerId(container.GetId())
						u.SetLinuxMemoryLimit(updatePluginMemoryLimit)

						return []*nri.ContainerUpdate{u}, nil
					},
				)

				By("updating the running container's resources via CRI")
				Expect(
					rc.UpdateContainerResources(ctx, containerID, updateRequestResources()),
				).To(Succeed())

				expectOnePostUpdate(plugin, containerID)

				By("verifying PostUpdateContainer reflects the plugin's adjustment")

				post := plugin.eventsFor(updateEventPostUpdate, containerID)[0]
				expectIdentity(post)
				Expect(post.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updatePluginMemoryLimit),
					"the plugin's memory limit MUST supersede the CRI-requested one")
				expectSameCPUShares(int64(post.Resources.GetCpu().GetShares().GetValue()),
					updateRequestCPUShares,
					"CRI-requested CPU shares not touched by the plugin MUST be preserved")

				By("verifying ContainerStatus reports the adjusted resources")

				after := statusLinuxResources(ctx, rc, containerID)

				if before == nil {
					Skip("ContainerStatus does not report Resources; the runtime does not " +
						"expose applied resources, so the plugin adjustment cannot be verified via CRI")
				}

				Expect(after).NotTo(BeNil(),
					"ContainerStatus reported Resources before the update and MUST still report them")
				Expect(after.GetMemoryLimitInBytes()).To(Equal(updatePluginMemoryLimit),
					"ContainerStatus MUST report the memory limit set by the NRI plugin")
				expectSameCPUShares(after.GetCpuShares(), updateRequestCPUShares,
					"ContainerStatus MUST report the CRI-requested CPU shares")
			},
		)

		// An error from UpdateContainer fails the CRI call: no
		// PostUpdateContainer is delivered and the resources are not changed.
		It("should fail UpdateContainerResources and keep resources when the plugin errors",
			func(ctx SpecContext) {
				before := statusLinuxResources(ctx, rc, containerID)

				plugin.setOnUpdate(
					func(_ *nri.Container, _ *nri.LinuxResources) ([]*nri.ContainerUpdate, error) {
						return nil, errors.New("induced NRI UpdateContainer failure")
					},
				)

				By("updating the running container's resources via CRI")

				err := rc.UpdateContainerResources(ctx, containerID, updateRequestResources())
				Expect(err).To(HaveOccurred(),
					"UpdateContainerResources MUST fail when the NRI UpdateContainer hook returns an error")

				// Read the resources right when the call returns: a failed
				// update must leave them unchanged, not restore them later.
				after := statusLinuxResources(ctx, rc, containerID)

				By(
					"verifying the failed call delivered UpdateContainer at most once and no PostUpdateContainer",
				)
				Expect(plugin.eventsFor(updateEventUpdate, containerID)).To(HaveLen(1),
					"the NRI UpdateContainer hook MUST have been called before the failure")
				Consistently(func() []string {
					return plugin.eventTypesFor(containerID)
				}, 2*time.Second, 200*time.Millisecond).Should(Equal([]string{updateEventUpdate}),
					"a failed update MUST deliver UpdateContainer at most once and no PostUpdateContainer")

				By(
					"verifying ContainerStatus reported the original resources when the call returned",
				)

				if before != nil {
					Expect(after).NotTo(BeNil(),
						"ContainerStatus reported Resources before the update and MUST still report them")
					expectSameCPUShares(after.GetCpuShares(), updateInitialCPUShares,
						"CPU shares MUST be unchanged after a failed update")
					Expect(after.GetMemoryLimitInBytes()).To(Equal(updateInitialMemoryLimit),
						"memory limit MUST be unchanged after a failed update")
					Expect(after.GetCpuShares()).To(Equal(before.GetCpuShares()),
						"CPU shares MUST be unchanged after a failed update")
					Expect(after.GetMemoryLimitInBytes()).To(Equal(before.GetMemoryLimitInBytes()),
						"memory limit MUST be unchanged after a failed update")
				}

				By("verifying the plugin still observes a successful update (positive control)")
				plugin.setOnUpdate(nil)
				Expect(
					rc.UpdateContainerResources(ctx, containerID, updateRequestResources()),
				).To(Succeed())
				expectOnePostUpdate(plugin, containerID)

				if before == nil {
					Skip("ContainerStatus does not report Resources; the runtime does not " +
						"expose applied resources, so unchanged resources cannot be verified via CRI")
				}
			})

		// An error from PostUpdateContainer does not undo the update and does
		// not fail the CRI call: the update has already been applied.
		It("should keep the update and succeed when PostUpdateContainer errors",
			func(ctx SpecContext) {
				before := statusLinuxResources(ctx, rc, containerID)

				plugin.setPostUpdateErr(errors.New("induced NRI PostUpdateContainer failure"))

				By("updating the running container's resources via CRI")
				Expect(
					rc.UpdateContainerResources(ctx, containerID, updateRequestResources()),
				).To(Succeed(),
					"UpdateContainerResources MUST NOT fail when the NRI PostUpdateContainer hook returns an error")

				expectOnePostUpdate(plugin, containerID)
				expectIdentity(plugin.eventsFor(updateEventPostUpdate, containerID)[0])

				By("verifying ContainerStatus reports the updated resources")

				after := statusLinuxResources(ctx, rc, containerID)

				if before == nil {
					Skip("ContainerStatus does not report Resources; the runtime does not " +
						"expose applied resources, so the kept update cannot be verified via CRI")
				}

				Expect(after).NotTo(BeNil(),
					"ContainerStatus reported Resources before the update and MUST still report them")
				expectSameCPUShares(after.GetCpuShares(), updateRequestCPUShares,
					"CPU shares MUST stay updated after a PostUpdateContainer error")
				Expect(after.GetMemoryLimitInBytes()).To(Equal(updateRequestMemoryLimit),
					"memory limit MUST stay updated after a PostUpdateContainer error")
			})
	})
})
