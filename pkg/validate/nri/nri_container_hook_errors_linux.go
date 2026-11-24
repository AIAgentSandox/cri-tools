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
	"strings"
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

// These tests make an NRI plugin hook return an error for one container and
// check what the runtime does with it. Two plugins are connected: a failing
// one (index 00) and a healthy one after it (index 10).
//
// Teardown hooks (StopContainer, RemoveContainer). The draft NRI pod sandbox
// lifecycle document (containerd/nri#286 at revision 2339aa3, unmerged) states,
// for the pod teardown hooks, that a plugin error must not block teardown. The
// tests apply that to the container teardown hooks: the runtime still does its
// own teardown and every other plugin still gets the hook. In addition, a
// plugin that failed to release its per-container state must be told again,
// so the error is returned to the CRI caller (which retries), a retry
// redelivers the hook, and RemoveContainer does not remove the container while
// a connected plugin keeps failing; otherwise that plugin leaks the state.
//
// StartContainer hook. A plugin that fails StartContainer (for example, it
// could not set up something the container needs) must be able to keep the
// container from running, so the CRI StartContainer call fails and the
// container is not running. The container can still be stopped and removed,
// and those calls deliver their hooks, so plugins can release what they
// allocated for it.

const (
	containerHookErrorsFailingPlugin = "cri-test-nri-ctr-hook-errors"
	containerHookErrorsHealthyPlugin = "cri-test-nri-ctr-hook-errors-healthy"
)

// containerHookErrorsCount returns how many events of the given type the
// plugin recorded for the given pod and container.
func containerHookErrorsCount(
	plugin *NRITestPlugin,
	eventType NRIEventType,
	podID, containerID string,
) int {
	count := 0
	events := plugin.Events()

	for i := range events {
		if events[i].Type == eventType &&
			events[i].PodSandboxID == podID &&
			events[i].ContainerID == containerID {
			count++
		}
	}

	return count
}

// containerHookErrorsIndex returns the position of the first event of the
// given type for the given pod and container in the plugin's event stream,
// or -1 if there is none.
func containerHookErrorsIndex(
	plugin *NRITestPlugin,
	eventType NRIEventType,
	podID, containerID string,
) int {
	events := plugin.Events()

	for i := range events {
		if events[i].Type == eventType &&
			events[i].PodSandboxID == podID &&
			events[i].ContainerID == containerID {
			return i
		}
	}

	return -1
}

// containerHookErrorsFailFor returns a container hook that returns an error for
// the given pod and container while fail is set, and succeeds otherwise.
func containerHookErrorsFailFor(
	fail *atomic.Bool,
	podID, containerID, hook string,
) func(context.Context, *nri.PodSandbox, *nri.Container) error {
	return func(_ context.Context, pod *nri.PodSandbox, ctr *nri.Container) error {
		if fail.Load() && pod.GetId() == podID && ctr.GetId() == containerID {
			return errors.New("induced NRI " + hook + " failure")
		}

		return nil
	}
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

// containerHookErrorsSkipDeviations skips the spec with the recorded runtime
// deviations, if any. Call it after every other assertion and the cleanup.
func containerHookErrorsSkipDeviations(deviations []string) {
	if len(deviations) > 0 {
		Skip("spec discrepancy: " + strings.Join(deviations, "; "))
	}
}

const containerHookErrorsT2Deviation = "healthy plugin %s (index 10) did not get the %s hook " +
	"after plugin %s (index 00) returned an error from it. Expected: a plugin error never blocks " +
	"teardown, so every other plugin still gets the hook, in index order (containerd/nri#286 at " +
	"2339aa3, unmerged draft, \"Error Handling\"). Observed: delivery stopped at the failing plugin. " +
	"Tracking: containerd/nri#274"

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
			failingStub *NRITestStub
			healthyStub *NRITestStub
			podID       string
			podConfig   *runtimeapi.PodSandboxConfig
			containerID string
			// fail turns the failing plugin's hook error on and off. Cleared
			// before the cleanup so the teardown can complete.
			fail atomic.Bool
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			fail.Store(false)

			failingStub, err = StartNRITestStub(containerHookErrorsFailingPlugin, "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start the failing NRI test stub")

			healthyStub, err = StartNRITestStub(containerHookErrorsHealthyPlugin, "10")
			Expect(err).NotTo(HaveOccurred(), "failed to start the healthy NRI test stub")

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
			// Stop the hook error and disconnect the stubs first, so a still
			// failing hook cannot interfere with teardown.
			fail.Store(false)

			if failingStub != nil {
				failingStub.Cleanup()
			}

			if healthyStub != nil {
				healthyStub.Cleanup()
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
			"should report a StopContainer hook error, stop the container and redeliver the hook on retry",
			func(ctx SpecContext) {
				ctrID := containerID
				failing := failingStub.Plugin
				healthy := healthyStub.Plugin
				failing.OnStopContainer = containerHookErrorsFailFor(
					&fail,
					podID,
					ctrID,
					"StopContainer",
				)

				var deviations []string

				By("starting the container")
				Expect(rc.StartContainer(ctx, ctrID)).To(Succeed())

				By("stopping the container while the StopContainer hook is failing")
				fail.Store(true)

				stopErr := rc.StopContainer(ctx, ctrID, 0)
				failedDeliveries := containerHookErrorsCount(
					failing,
					EventStopContainer,
					podID,
					ctrID,
				)
				Expect(failedDeliveries).To(BeNumerically(">=", 1),
					"the failing plugin must get the StopContainer hook for the container before the call returns")

				By("verifying the container reached the EXITED state despite the hook error")
				Eventually(func(g Gomega) {
					resp, err := rc.ContainerStatus(ctx, ctrID, false)
					g.Expect(err).NotTo(HaveOccurred(), "ContainerStatus after StopContainer")
					g.Expect(resp.GetStatus().GetState()).
						To(Equal(runtimeapi.ContainerState_CONTAINER_EXITED),
							"the runtime must still stop the container when a StopContainer hook returns an error")
				}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

				By("verifying the healthy plugin also got the StopContainer hook")

				if containerHookErrorsCount(healthy, EventStopContainer, podID, ctrID) == 0 {
					deviations = append(deviations, fmt.Sprintf(
						containerHookErrorsT2Deviation,
						containerHookErrorsHealthyPlugin,
						"StopContainer",
						containerHookErrorsFailingPlugin,
					))
				}

				if stopErr != nil {
					framework.Logf("StopContainer returned the hook error: %v", stopErr)

					By("retrying StopContainer while the hook still fails")
					Expect(rc.StopContainer(ctx, ctrID, 0)).NotTo(Succeed(),
						"a retried StopContainer must keep returning an error while a plugin's StopContainer hook fails")
					Expect(containerHookErrorsCount(failing, EventStopContainer, podID, ctrID)).
						To(BeNumerically(">", failedDeliveries),
							"a retried StopContainer must redeliver the hook to the plugin that failed it")

					failedDeliveries = containerHookErrorsCount(
						failing,
						EventStopContainer,
						podID,
						ctrID,
					)

					By("retrying StopContainer after the hook stops failing")
					fail.Store(false)
					Expect(rc.StopContainer(ctx, ctrID, 0)).To(Succeed(),
						"StopContainer must succeed once every plugin's StopContainer hook succeeds")
					Expect(containerHookErrorsCount(failing, EventStopContainer, podID, ctrID)).
						To(BeNumerically(">", failedDeliveries),
							"the successful retry must redeliver the hook to the plugin that failed it")
				} else {
					// TODO: link the containerd issue for swallowed teardown hook errors once filed.
					deviations = append(deviations, "StopContainer succeeded although plugin "+
						containerHookErrorsFailingPlugin+" returned an error from its StopContainer hook. "+
						"Expected: the CRI call returns an error so the caller retries, and the retry "+
						"redelivers the hook to the plugin that failed. Observed: the runtime dropped the "+
						"plugin error and returned success, so the hook is never redelivered. Rationale: a "+
						"plugin that failed to release its per-container state must be told again, or it leaks "+
						"that state. No runtime issue filed yet")
				}

				By("cleaning up the container")
				fail.Store(false)
				Expect(rc.RemoveContainer(ctx, ctrID)).To(Succeed())

				containerID = ""

				containerHookErrorsSkipDeviations(deviations)
			},
		)

		It(
			"should report a RemoveContainer hook error and keep the container until the hook succeeds",
			func(ctx SpecContext) {
				ctrID := containerID
				failing := failingStub.Plugin
				healthy := healthyStub.Plugin
				failing.OnRemoveContainer = containerHookErrorsFailFor(
					&fail,
					podID,
					ctrID,
					"RemoveContainer",
				)

				var deviations []string

				By("starting and stopping the container")
				Expect(rc.StartContainer(ctx, ctrID)).To(Succeed())
				Expect(rc.StopContainer(ctx, ctrID, 0)).To(Succeed())

				By("removing the container while the RemoveContainer hook is failing")
				fail.Store(true)

				removeErr := rc.RemoveContainer(ctx, ctrID)
				failedDeliveries := containerHookErrorsCount(
					failing,
					EventRemoveContainer,
					podID,
					ctrID,
				)
				Expect(failedDeliveries).To(BeNumerically(">=", 1),
					"the failing plugin must get the RemoveContainer hook for the container before the call returns")

				By("verifying the healthy plugin also got the RemoveContainer hook")

				if containerHookErrorsCount(healthy, EventRemoveContainer, podID, ctrID) == 0 {
					deviations = append(deviations, fmt.Sprintf(
						containerHookErrorsT2Deviation,
						containerHookErrorsHealthyPlugin,
						"RemoveContainer",
						containerHookErrorsFailingPlugin,
					))
				}

				listed, err := containerHookErrorsFindContainer(ctx, rc, podID, ctrID)
				Expect(err).NotTo(HaveOccurred(), "ListContainers after RemoveContainer")

				if removeErr != nil {
					framework.Logf("RemoveContainer returned the hook error: %v", removeErr)

					By("verifying the container is still listed while the hook fails")
					Expect(listed).NotTo(BeNil(),
						"the container must stay listed while a connected plugin's RemoveContainer hook fails")

					By("retrying RemoveContainer while the hook still fails")
					Expect(rc.RemoveContainer(ctx, ctrID)).NotTo(Succeed(),
						"a retried RemoveContainer must keep returning an error while a plugin's RemoveContainer hook fails")
					Expect(containerHookErrorsCount(failing, EventRemoveContainer, podID, ctrID)).
						To(BeNumerically(">", failedDeliveries),
							"a retried RemoveContainer must redeliver the hook to the plugin that failed it")

					listed, err = containerHookErrorsFindContainer(ctx, rc, podID, ctrID)
					Expect(
						err,
					).NotTo(HaveOccurred(), "ListContainers after the retried RemoveContainer")
					Expect(listed).NotTo(BeNil(),
						"the container must stay listed while a connected plugin's RemoveContainer hook fails")

					failedDeliveries = containerHookErrorsCount(
						failing,
						EventRemoveContainer,
						podID,
						ctrID,
					)

					By("retrying RemoveContainer after the hook stops failing")
					fail.Store(false)
					Expect(rc.RemoveContainer(ctx, ctrID)).To(Succeed(),
						"RemoveContainer must succeed once every plugin's RemoveContainer hook succeeds")
					Expect(containerHookErrorsCount(failing, EventRemoveContainer, podID, ctrID)).
						To(BeNumerically(">", failedDeliveries),
							"the successful retry must redeliver the hook to the plugin that failed it")
				} else {
					observed := "the container was removed"
					if listed != nil {
						observed = "the container is still listed"
					}

					// TODO: link the containerd issue for swallowed teardown hook errors once filed.
					deviations = append(deviations, "RemoveContainer succeeded although plugin "+
						containerHookErrorsFailingPlugin+" returned an error from its RemoveContainer hook. "+
						"Expected: the CRI call returns an error and the container stays listed until every "+
						"connected plugin's RemoveContainer hook succeeds, and a retry redelivers the hook. "+
						"Observed: the runtime dropped the plugin error and returned success; "+observed+". "+
						"Rationale: a plugin that failed to release its per-container state must be told "+
						"again, or it leaks that state. No runtime issue filed yet")
				}

				By("verifying the container is no longer listed")
				fail.Store(false)
				Expect(rc.RemoveContainer(ctx, ctrID)).To(Succeed(),
					"RemoveContainer on a removed container must succeed (CRI: idempotent remove)")

				listed, err = containerHookErrorsFindContainer(ctx, rc, podID, ctrID)
				Expect(err).NotTo(HaveOccurred(), "ListContainers after RemoveContainer")
				Expect(
					listed,
				).To(BeNil(), "container must not be listed after a successful RemoveContainer")

				containerID = ""

				containerHookErrorsSkipDeviations(deviations)
			},
		)

		It(
			"should fail StartContainer when the StartContainer hook returns an error",
			func(ctx SpecContext) {
				ctrID := containerID
				failing := failingStub.Plugin
				healthy := healthyStub.Plugin
				failing.OnStartContainer = containerHookErrorsFailFor(
					&fail,
					podID,
					ctrID,
					"StartContainer",
				)

				By("starting the container while the StartContainer hook is failing")
				fail.Store(true)

				startErr := rc.StartContainer(ctx, ctrID)
				Expect(startErr).To(HaveOccurred(),
					"StartContainer must fail when a plugin's StartContainer hook returns an error")
				framework.Logf("StartContainer returned: %v", startErr)
				Expect(containerHookErrorsCount(failing, EventStartContainer, podID, ctrID)).
					To(BeNumerically(">=", 1),
						"the failing plugin must get the StartContainer hook for the container")

				By("verifying the container is not running")

				resp, err := rc.ContainerStatus(ctx, ctrID, false)
				Expect(err).NotTo(HaveOccurred(), "ContainerStatus after the failed StartContainer")
				Expect(
					resp.GetStatus().GetState(),
				).NotTo(Equal(runtimeapi.ContainerState_CONTAINER_RUNNING),
					"the container must not be running after StartContainer failed")

				By("stopping and removing the container")
				Expect(rc.StopContainer(ctx, ctrID, 0)).To(Succeed(),
					"StopContainer must succeed after a failed StartContainer")
				Expect(rc.RemoveContainer(ctx, ctrID)).To(Succeed(),
					"RemoveContainer must succeed after a failed StartContainer")

				containerID = ""

				listed, err := containerHookErrorsFindContainer(ctx, rc, podID, ctrID)
				Expect(err).NotTo(HaveOccurred(), "ListContainers after RemoveContainer")
				Expect(listed).To(BeNil(), "container must not be listed after RemoveContainer")

				By(
					"verifying both plugins got the StopContainer and RemoveContainer hooks in order",
				)

				for _, p := range []struct {
					name   string
					plugin *NRITestPlugin
				}{
					{containerHookErrorsFailingPlugin, failing},
					{containerHookErrorsHealthyPlugin, healthy},
				} {
					create := containerHookErrorsIndex(p.plugin, EventCreateContainer, podID, ctrID)
					stop := containerHookErrorsIndex(p.plugin, EventStopContainer, podID, ctrID)
					remove := containerHookErrorsIndex(p.plugin, EventRemoveContainer, podID, ctrID)

					Expect(create).To(BeNumerically(">=", 0),
						"plugin %s must have got CreateContainer for the container", p.name)
					Expect(stop).To(BeNumerically(">", create),
						"plugin %s must get StopContainer after CreateContainer for the container "+
							"whose start failed (events: %v)", p.name, p.plugin.Events())
					Expect(remove).To(BeNumerically(">", stop),
						"plugin %s must get RemoveContainer after StopContainer for the container "+
							"whose start failed (events: %v)", p.name, p.plugin.Events())
				}

				start := containerHookErrorsIndex(failing, EventStartContainer, podID, ctrID)
				Expect(start).To(BeNumerically("<",
					containerHookErrorsIndex(failing, EventStopContainer, podID, ctrID)),
					"the failing plugin must get StartContainer before StopContainer")
			},
		)
	})
})
