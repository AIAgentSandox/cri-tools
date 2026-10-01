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
	Type         string
	PodSandboxID string
	ContainerID  string
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

	return nil
}

func (p *updateTestPlugin) setOnUpdate(
	hook func(container *nri.Container, req *nri.LinuxResources) ([]*nri.ContainerUpdate, error),
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onUpdate = hook
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
		Type:         eventType,
		PodSandboxID: pod.GetId(),
		ContainerID:  container.GetId(),
		Resources:    res,
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

	s, err := stub.New(plugin,
		stub.WithPluginName("cri-test-nri-update"),
		stub.WithPluginIdx("00"),
		stub.WithSocketPath(socketPath),
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
		case <-time.After(5 * time.Second):
			s.Stop()
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

	return resp.GetStatus().GetResources().GetLinux()
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
			containerID string
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

			podConfig := &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(
					"nri-test-update-"+framework.NewUUID(),
					framework.DefaultUIDPrefix+framework.NewUUID(),
					framework.DefaultNamespacePrefix+framework.NewUUID(),
					framework.DefaultAttempt,
				),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}
			podID = framework.RunPodSandbox(ctx, rc, podConfig)

			By("creating and starting a container with initial resources")

			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(
					"nri-test-update-ctr-"+framework.NewUUID(),
					framework.DefaultAttempt,
				),
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

		It("should deliver UpdateContainer then PostUpdateContainer with the requested resources",
			func(ctx SpecContext) {
				By("updating the running container's resources via CRI")
				Expect(
					rc.UpdateContainerResources(ctx, containerID, updateRequestResources()),
				).To(Succeed())

				By("waiting for the PostUpdateContainer event")
				Eventually(func() int {
					return len(plugin.eventsFor(updateEventPostUpdate, containerID))
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"exactly one PostUpdateContainer event is expected")

				By("verifying event order")
				Expect(plugin.eventTypesFor(containerID)).To(Equal(
					[]string{updateEventUpdate, updateEventPostUpdate}),
					"UpdateContainer MUST be delivered before PostUpdateContainer")

				By("verifying the UpdateContainer event carries the IDs and requested resources")

				update := plugin.eventsFor(updateEventUpdate, containerID)[0]
				Expect(update.PodSandboxID).To(Equal(podID))
				Expect(update.Resources).NotTo(BeNil(),
					"UpdateContainer MUST carry the requested Linux resources")
				Expect(update.Resources.GetCpu().GetShares().GetValue()).To(
					BeEquivalentTo(updateRequestCPUShares),
					"UpdateContainer MUST carry the requested CPU shares")
				Expect(update.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updateRequestMemoryLimit),
					"UpdateContainer MUST carry the requested memory limit")

				By("verifying the PostUpdateContainer event carries the IDs and updated resources")

				post := plugin.eventsFor(updateEventPostUpdate, containerID)[0]
				Expect(post.PodSandboxID).To(Equal(podID))
				Expect(post.Resources).NotTo(BeNil(),
					"PostUpdateContainer MUST carry the container's Linux resources")
				Expect(post.Resources.GetCpu().GetShares().GetValue()).To(
					BeEquivalentTo(updateRequestCPUShares),
					"PostUpdateContainer MUST reflect the updated CPU shares")
				Expect(post.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updateRequestMemoryLimit),
					"PostUpdateContainer MUST reflect the updated memory limit")
			})

		It(
			"should apply resources adjusted by the plugin in UpdateContainer",
			func(ctx SpecContext) {
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

				Eventually(func() int {
					return len(plugin.eventsFor(updateEventPostUpdate, containerID))
				}, 10*time.Second, 50*time.Millisecond).Should(Equal(1),
					"exactly one PostUpdateContainer event is expected")

				By("verifying PostUpdateContainer reflects the plugin's adjustment")

				post := plugin.eventsFor(updateEventPostUpdate, containerID)[0]
				Expect(post.Resources.GetMemory().GetLimit().GetValue()).To(
					Equal(updatePluginMemoryLimit),
					"the plugin's memory limit MUST supersede the CRI-requested one")
				Expect(post.Resources.GetCpu().GetShares().GetValue()).To(
					BeEquivalentTo(updateRequestCPUShares),
					"CRI-requested resources not touched by the plugin MUST be preserved")

				By("verifying ContainerStatus reports the adjusted resources")

				res := statusLinuxResources(ctx, rc, containerID)
				if res == nil {
					Skip("ContainerStatus does not report Resources; the runtime does not " +
						"expose applied resources, so the plugin adjustment cannot be verified via CRI")
				}

				Expect(res.GetMemoryLimitInBytes()).To(Equal(updatePluginMemoryLimit),
					"ContainerStatus MUST report the memory limit set by the NRI plugin")
				Expect(res.GetCpuShares()).To(Equal(updateRequestCPUShares),
					"ContainerStatus MUST report the CRI-requested CPU shares")
			},
		)

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

				Expect(plugin.eventsFor(updateEventUpdate, containerID)).To(HaveLen(1),
					"the NRI UpdateContainer hook should have fired before the failure")

				By("verifying no PostUpdateContainer event is delivered for the failed update")
				Consistently(func() int {
					return len(plugin.eventsFor(updateEventPostUpdate, containerID))
				}, 2*time.Second, 200*time.Millisecond).Should(BeZero(),
					"PostUpdateContainer MUST NOT be delivered for a failed update")

				By("verifying ContainerStatus reports the original resources")

				after := statusLinuxResources(ctx, rc, containerID)
				if before == nil || after == nil {
					Skip("ContainerStatus does not report Resources; the runtime does not " +
						"expose applied resources, so unchanged resources cannot be verified via CRI")
				}

				Expect(after.GetCpuShares()).To(Equal(updateInitialCPUShares),
					"CPU shares MUST be unchanged after a failed update")
				Expect(after.GetMemoryLimitInBytes()).To(Equal(updateInitialMemoryLimit),
					"memory limit MUST be unchanged after a failed update")
				Expect(after.GetCpuShares()).To(Equal(before.GetCpuShares()))
				Expect(after.GetMemoryLimitInBytes()).To(Equal(before.GetMemoryLimitInBytes()))
			})
	})
})
