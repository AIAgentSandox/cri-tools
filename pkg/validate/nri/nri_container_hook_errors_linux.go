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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

// The NRI pod sandbox lifecycle spec (containerd/nri docs/pod-sandbox-lifecycle.md,
// "Error Handling") states that on the teardown path plugin errors MUST NOT
// prevent the operation from proceeding, and that runtimes MUST ensure a
// failing plugin cannot block pod or container teardown. The per-hook rules in
// that section are spelled out for StopPodSandbox and RemovePodSandbox only;
// the tests below apply them by analogy to the StopContainer and
// RemoveContainer hooks. No rule is given for errors returned from the
// StartContainer hook, so its outcome is only observed and reported.
//
// All three hooks can return an error through the NRI stub API: the stub
// relays the handler error in the ttrpc response and the NRI adaptation
// returns any non-fatal plugin error to the runtime, which decides how to act
// on it. None of these events is notification-only at the protocol level.

// containerHookErrorsCount returns how many events of the given type were
// recorded by the plugin for the given container.
func containerHookErrorsCount(
	plugin *NRITestPlugin,
	eventType NRIEventType,
	containerID string,
) int {
	count := 0
	events := plugin.Events()

	for i := range events {
		if events[i].Type == eventType && events[i].ContainerID == containerID {
			count++
		}
	}

	return count
}

// containerHookErrorsFindContainer returns the container with the given ID
// from ListContainers in the given sandbox, or nil if it is not listed.
func containerHookErrorsFindContainer(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podID, containerID string,
) (*runtimeapi.Container, error) {
	containers, err := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
		PodSandboxId: podID,
	})
	if err != nil {
		return nil, err
	}

	for _, c := range containers {
		if c.GetId() == containerID {
			return c, nil
		}
	}

	return nil, nil
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

	Context("container hook errors", Serial, func() {
		var (
			testStub    *NRITestStub
			podID       string
			podConfig   *runtimeapi.PodSandboxConfig
			containerID string
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-ctr-hook-errors", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			framework.PullPublicImage(
				ctx,
				ic,
				framework.TestContext.TestImageList.DefaultTestContainerImage,
				nil,
			)

			By("creating a pod sandbox")

			podSandboxName := "nri-test-ctr-hook-errors-" + framework.NewUUID()
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
			podID = framework.RunPodSandbox(ctx, rc, podConfig)
			Expect(podID).NotTo(BeEmpty())

			By("creating a container in the sandbox")

			containerConfig := &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(
					"nri-test-ctr-hook-errors-ctr-"+framework.NewUUID(),
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
		})

		AfterEach(func(ctx SpecContext) {
			// Stop the stub first so a still-failing hook cannot interfere with
			// teardown of the container or sandbox.
			if testStub != nil {
				testStub.Cleanup()
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

		It(
			"should stop the container when the StopContainer hook returns an error",
			func(ctx SpecContext) {
				testStub.Plugin.OnStopContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					return errors.New("induced NRI StopContainer failure")
				}

				By("starting the container")
				Expect(rc.StartContainer(ctx, containerID)).NotTo(HaveOccurred())

				By("stopping the container while the StopContainer hook is failing")
				Expect(rc.StopContainer(ctx, containerID, 0)).NotTo(HaveOccurred(),
					"StopContainer MUST succeed even when the NRI StopContainer hook returns an error")

				By("verifying the NRI StopContainer hook fired")
				Eventually(func() int {
					return containerHookErrorsCount(
						testStub.Plugin,
						EventStopContainer,
						containerID,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
					"NRI StopContainer hook should have fired for the container")

				By("verifying the container reached the EXITED state")
				Eventually(func() runtimeapi.ContainerState {
					resp, err := rc.ContainerStatus(ctx, containerID, false)
					Expect(err).NotTo(HaveOccurred(), "ContainerStatus after StopContainer")

					return resp.GetStatus().GetState()
				}, 30*time.Second, 200*time.Millisecond).Should(Equal(runtimeapi.ContainerState_CONTAINER_EXITED),
					"container MUST reach EXITED even when the NRI StopContainer hook returns an error")
			},
		)

		It(
			"should remove the container when the RemoveContainer hook returns an error",
			func(ctx SpecContext) {
				testStub.Plugin.OnRemoveContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					return errors.New("induced NRI RemoveContainer failure")
				}

				By("starting and stopping the container")
				Expect(rc.StartContainer(ctx, containerID)).NotTo(HaveOccurred())
				Expect(rc.StopContainer(ctx, containerID, 0)).NotTo(HaveOccurred())

				By("removing the container while the RemoveContainer hook is failing")
				Expect(rc.RemoveContainer(ctx, containerID)).NotTo(HaveOccurred(),
					"RemoveContainer MUST succeed even when the NRI RemoveContainer hook returns an error")

				removedID := containerID
				// The container is gone; clear so AfterEach does not touch it again.
				containerID = ""

				By("verifying the NRI RemoveContainer hook fired")
				Eventually(func() int {
					return containerHookErrorsCount(
						testStub.Plugin,
						EventRemoveContainer,
						removedID,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
					"NRI RemoveContainer hook should have fired for the container")

				By("verifying the container is no longer listed")

				c, err := containerHookErrorsFindContainer(ctx, rc, podID, removedID)
				Expect(err).NotTo(HaveOccurred(), "ListContainers after RemoveContainer")
				Expect(c).To(BeNil(),
					"container MUST NOT be listed after RemoveContainer, even when the NRI RemoveContainer hook returns an error")
			},
		)

		It(
			"should report the outcome when the StartContainer hook returns an error",
			func(ctx SpecContext) {
				testStub.Plugin.OnStartContainer = func(_ context.Context, _ *nri.PodSandbox, _ *nri.Container) error {
					return errors.New("induced NRI StartContainer failure")
				}

				By("starting the container while the StartContainer hook is failing")

				startErr := rc.StartContainer(ctx, containerID)

				By("verifying the NRI StartContainer hook fired")
				Eventually(func() int {
					return containerHookErrorsCount(
						testStub.Plugin,
						EventStartContainer,
						containerID,
					)
				}, 10*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
					"NRI StartContainer hook should have fired for the container")

				By("observing the container state after StartContainer")

				resp, err := rc.ContainerStatus(ctx, containerID, false)
				Expect(err).NotTo(HaveOccurred(), "ContainerStatus after StartContainer")

				observedState := resp.GetStatus().GetState()

				By("verifying the container can still be stopped and removed")
				// Whatever the start outcome, teardown MUST NOT be blocked: the
				// container must remain stoppable and removable. The StartContainer
				// hook keeps failing, but it is not on the teardown path.
				Expect(rc.StopContainer(ctx, containerID, 0)).NotTo(HaveOccurred(),
					"StopContainer MUST succeed after a StartContainer NRI hook error")
				Expect(rc.RemoveContainer(ctx, containerID)).NotTo(HaveOccurred(),
					"RemoveContainer MUST succeed after a StartContainer NRI hook error")

				removedID := containerID
				containerID = ""

				c, err := containerHookErrorsFindContainer(ctx, rc, podID, removedID)
				Expect(err).NotTo(HaveOccurred(), "ListContainers after RemoveContainer")
				Expect(c).To(BeNil(), "container MUST NOT be listed after RemoveContainer")

				// The spec does not define whether a StartContainer hook error must
				// fail the CRI StartContainer call or which state the container is
				// left in (the closest rule, for RunPodSandbox, leaves it to runtime
				// policy). Report what the runtime did instead of asserting it.
				outcome := "CRI StartContainer succeeded (the plugin error was ignored)"
				if startErr != nil {
					outcome = fmt.Sprintf("CRI StartContainer failed with: %v", startErr)
				}

				Skip(fmt.Sprintf(
					"unspecified behavior: the NRI spec does not define the outcome of a StartContainer "+
						"hook error. Observed: %s; container state afterwards was %s. "+
						"Teardown (StopContainer/RemoveContainer) succeeded as required.",
					outcome,
					observedState,
				))
			},
		)
	})
})
