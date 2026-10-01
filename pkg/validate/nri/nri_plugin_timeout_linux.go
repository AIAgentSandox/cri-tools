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
	"maps"
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

const (
	// pluginTimeoutLabelKey labels every sandbox created by the plugin timeout
	// specs so AfterEach can find and remove any sandbox a failed or
	// timed-out CRI call may have left behind.
	pluginTimeoutLabelKey = "cri-tools.nri-plugin-timeout"

	// pluginTimeoutMaxRequestTimeout bounds the runtime NRI request timeout the
	// specs are willing to wait out. The timeout is read from the value the
	// runtime advertises to the plugin during Configure (containerd's
	// plugin_request_timeout, default 2s); larger values would make the specs
	// needlessly slow, so they are skipped instead.
	pluginTimeoutMaxRequestTimeout = 30 * time.Second

	// pluginTimeoutCallSlack is the extra time, on top of the runtime NRI
	// request timeout, a CRI call is given to return once a hook stops
	// responding. It covers the non-NRI work of the call (sandbox setup and
	// teardown, container creation). A call still pending after that is treated
	// as the runtime waiting on the plugin beyond its own request timeout.
	pluginTimeoutCallSlack = 60 * time.Second

	// pluginTimeoutHookSafetyBound caps how long a blocking hook waits for the
	// test to release it, so a hook can never outlive the spec indefinitely
	// even if AfterEach is not reached.
	pluginTimeoutHookSafetyBound = 3 * time.Minute
)

// pluginTimeoutGate blocks the first hook invocation routed through it until
// released, simulating an unresponsive plugin. It deliberately ignores the
// hook context so that the runtime, not the plugin, must enforce the request
// timeout. Subsequent invocations (e.g. retries) pass through immediately.
type pluginTimeoutGate struct {
	first       sync.Once
	releaseOnce sync.Once
	reached     chan struct{}
	release     chan struct{}
}

func newPluginTimeoutGate() *pluginTimeoutGate {
	return &pluginTimeoutGate{
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *pluginTimeoutGate) block() error {
	isFirst := false

	g.first.Do(func() { isFirst = true })

	if !isFirst {
		return nil
	}

	close(g.reached)

	select {
	case <-g.release:
	case <-time.After(pluginTimeoutHookSafetyBound):
	}

	return nil
}

func (g *pluginTimeoutGate) wasReached() bool {
	select {
	case <-g.reached:
		return true
	default:
		return false
	}
}

func (g *pluginTimeoutGate) unblock() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// pluginTimeoutPodConfig returns a pod sandbox config carrying a unique label
// value that identifies the sandbox for leak checks and cleanup.
func pluginTimeoutPodConfig(
	ctx context.Context,
	rc internalapi.RuntimeService,
	namePrefix, labelValue string,
) *runtimeapi.PodSandboxConfig {
	labels := maps.Clone(framework.DefaultPodLabels)
	if labels == nil {
		labels = map[string]string{}
	}

	labels[pluginTimeoutLabelKey] = labelValue

	return &runtimeapi.PodSandboxConfig{
		Metadata: framework.BuildPodSandboxMetadata(
			namePrefix+framework.NewUUID(),
			framework.DefaultUIDPrefix+framework.NewUUID(),
			framework.DefaultNamespacePrefix+framework.NewUUID(),
			framework.DefaultAttempt,
		),
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			CgroupParent: common.GetCgroupParent(ctx, rc),
		},
		Labels: labels,
	}
}

// pluginTimeoutListSandboxes lists the sandboxes carrying the given label value.
func pluginTimeoutListSandboxes(
	ctx context.Context,
	rc internalapi.RuntimeService,
	labelValue string,
) ([]*runtimeapi.PodSandbox, error) {
	return rc.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{
		LabelSelector: map[string]string{pluginTimeoutLabelKey: labelValue},
	})
}

// pluginTimeoutRequestTimeout returns the NRI request timeout the runtime
// advertised to the stub, skipping the spec when it is unknown or too large.
func pluginTimeoutRequestTimeout(testStub *NRITestStub) time.Duration {
	timeout := testStub.Stub.RequestTimeout()
	if timeout <= 0 {
		Skip("runtime did not advertise an NRI plugin request timeout during Configure; " +
			"cannot derive how long a hook must block to exceed it")
	}

	if timeout > pluginTimeoutMaxRequestTimeout {
		Skip("runtime NRI plugin request timeout " + timeout.String() +
			" exceeds " + pluginTimeoutMaxRequestTimeout.String() +
			"; skipping to keep the plugin timeout specs fast")
	}

	framework.Logf("runtime NRI plugin request timeout: %v", timeout)

	return timeout
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

	Context("plugin timeout", Serial, func() {
		var (
			testStub   *NRITestStub
			gate       *pluginTimeoutGate
			labelValue string
		)

		BeforeEach(func() {
			gate = newPluginTimeoutGate()
			labelValue = framework.NewUUID()
		})

		AfterEach(func(ctx SpecContext) {
			// Release any hook still blocked, then disconnect the plugin so
			// the cleanup CRI calls below cannot be held up by it.
			gate.unblock()

			if testStub != nil {
				testStub.Cleanup()
				testStub = nil
			}

			// Remove every sandbox created by the spec, including any left
			// behind by a failed or timed-out CRI call. RemovePodSandbox
			// also removes the sandbox's containers.
			pods, err := pluginTimeoutListSandboxes(ctx, rc, labelValue)
			if err != nil {
				framework.Logf("AfterEach: ListPodSandbox failed: %v", err)

				return
			}

			for _, pod := range pods {
				if err := rc.StopPodSandbox(ctx, pod.GetId()); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", pod.GetId(), err)
				}

				if err := rc.RemovePodSandbox(ctx, pod.GetId()); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", pod.GetId(), err)
				}
			}
		})

		It(
			"should not leak a sandbox when the RunPodSandbox hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
				// Spec: a plugin timeout is treated as an error; on RunPodSandbox
				// it may fail pod creation depending on runtime policy. Either
				// way the CRI call must return, and a failed call must not leave
				// a sandbox behind.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-run", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				requestTimeout := pluginTimeoutRequestTimeout(testStub)

				testStub.Plugin.OnRunPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				podConfig := pluginTimeoutPodConfig(ctx, rc, "nri-test-timeout-run-", labelValue)

				By(
					"running a pod sandbox while the RunPodSandbox hook blocks past the request timeout",
				)

				callCtx, cancel := context.WithTimeout(ctx, requestTimeout+pluginTimeoutCallSlack)
				defer cancel()

				podID, runErr := rc.RunPodSandbox(
					callCtx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				callTimedOut := callCtx.Err() != nil

				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "RunPodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"RunPodSandbox did not return within %v; the runtime must not wait on the plugin "+
						"beyond its NRI request timeout (%v)", requestTimeout+pluginTimeoutCallSlack, requestTimeout)

				if runErr == nil {
					// Runtime policy: the plugin timeout did not fail pod
					// creation (containerd closes the timed-out plugin and
					// proceeds). The sandbox must then be fully usable.
					framework.Logf(
						"RunPodSandbox succeeded despite the hook timeout (runtime policy): %s",
						podID,
					)

					status, statusErr := rc.PodSandboxStatus(ctx, podID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(
						status.GetStatus().GetState(),
					).To(Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
						"sandbox created despite a hook timeout must be ready")

					return
				}

				framework.Logf("RunPodSandbox failed after the hook timeout: %v", runErr)

				By("verifying the failed RunPodSandbox did not leak a sandbox")
				Eventually(func() ([]*runtimeapi.PodSandbox, error) {
					return pluginTimeoutListSandboxes(ctx, rc, labelValue)
				}, 30*time.Second, 500*time.Millisecond).Should(BeEmpty(),
					"a RunPodSandbox that failed due to a plugin timeout MUST NOT leave a sandbox behind")

				By("retrying RunPodSandbox with the same config")

				retryPodID, retryErr := rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
				Expect(retryErr).NotTo(HaveOccurred(),
					"RunPodSandbox retry should succeed once the plugin responds in time")
				Expect(retryPodID).NotTo(BeEmpty())
			},
		)

		It(
			"should not leak a container when the CreateContainer hook exceeds the request timeout, and a retry should succeed",
			func(ctx SpecContext) {
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-create", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				requestTimeout := pluginTimeoutRequestTimeout(testStub)

				By("creating a pod sandbox")

				podConfig := pluginTimeoutPodConfig(ctx, rc, "nri-test-timeout-create-", labelValue)
				podID := framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						"nri-test-timeout-create-ctr-"+framework.NewUUID(),
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				testStub.Plugin.OnCreateContainer = func(
					_ context.Context, _ *nri.PodSandbox, _ *nri.Container,
				) error {
					return gate.block()
				}

				By(
					"creating a container while the CreateContainer hook blocks past the request timeout",
				)

				callCtx, cancel := context.WithTimeout(ctx, requestTimeout+pluginTimeoutCallSlack)
				defer cancel()

				containerID, createErr := framework.CreateContainerWithError(
					callCtx, rc, ic, containerConfig, podID, podConfig,
				)
				callTimedOut := callCtx.Err() != nil

				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "CreateContainer NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"CreateContainer did not return within %v; the runtime must not wait on the plugin "+
						"beyond its NRI request timeout (%v)", requestTimeout+pluginTimeoutCallSlack, requestTimeout)

				if createErr == nil {
					framework.Logf(
						"CreateContainer succeeded despite the hook timeout (runtime policy): %s",
						containerID,
					)

					status, statusErr := rc.ContainerStatus(ctx, containerID, false)
					Expect(statusErr).NotTo(HaveOccurred())
					Expect(
						status.GetStatus().GetState(),
					).To(Equal(runtimeapi.ContainerState_CONTAINER_CREATED),
						"container created despite a hook timeout must be in CREATED state")

					return
				}

				framework.Logf("CreateContainer failed after the hook timeout: %v", createErr)

				By("verifying the failed CreateContainer did not leak a container")
				Eventually(func() ([]*runtimeapi.Container, error) {
					return rc.ListContainers(ctx, &runtimeapi.ContainerFilter{PodSandboxId: podID})
				}, 30*time.Second, 500*time.Millisecond).Should(BeEmpty(),
					"a CreateContainer that failed due to a plugin timeout MUST NOT leave a container behind")

				By("retrying CreateContainer with the same config")

				retryID, retryErr := framework.CreateContainerWithError(
					ctx, rc, ic, containerConfig, podID, podConfig,
				)
				Expect(retryErr).NotTo(HaveOccurred(),
					"CreateContainer retry with the same name should succeed once the plugin responds in time")
				Expect(retryID).NotTo(BeEmpty())
			},
		)

		It(
			"should stop and remove the sandbox when the StopPodSandbox hook exceeds the request timeout",
			func(ctx SpecContext) {
				// Spec: StopPodSandbox is non-blocking with respect to plugin
				// timeouts; a plugin error MUST NOT prevent the sandbox from
				// being stopped.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-stop", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				requestTimeout := pluginTimeoutRequestTimeout(testStub)

				By("creating a pod sandbox")

				podConfig := pluginTimeoutPodConfig(ctx, rc, "nri-test-timeout-stop-", labelValue)
				podID := framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				testStub.Plugin.OnStopPodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				By(
					"stopping the sandbox while the StopPodSandbox hook blocks past the request timeout",
				)

				callCtx, cancel := context.WithTimeout(ctx, requestTimeout+pluginTimeoutCallSlack)
				defer cancel()

				stopErr := rc.StopPodSandbox(callCtx, podID)
				callTimedOut := callCtx.Err() != nil

				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "StopPodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"StopPodSandbox did not return within %v; the runtime must not wait on the plugin "+
						"beyond its NRI request timeout (%v)", requestTimeout+pluginTimeoutCallSlack, requestTimeout)
				Expect(stopErr).NotTo(HaveOccurred(),
					"a StopPodSandbox hook timeout MUST NOT prevent the sandbox from being stopped")

				status, statusErr := rc.PodSandboxStatus(ctx, podID, false)
				Expect(statusErr).NotTo(HaveOccurred())
				Expect(
					status.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"sandbox MUST be stopped after StopPodSandbox returns")

				By("removing the sandbox")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after a StopPodSandbox hook timeout")

				Expect(pluginTimeoutListSandboxes(ctx, rc, labelValue)).To(BeEmpty(),
					"sandbox MUST be removed")
			},
		)

		It(
			"should remove the sandbox when the RemovePodSandbox hook exceeds the request timeout",
			func(ctx SpecContext) {
				// Spec: RemovePodSandbox is non-blocking; removal proceeds
				// regardless of a plugin timeout.
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-remove", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				requestTimeout := pluginTimeoutRequestTimeout(testStub)

				By("creating and stopping a pod sandbox")

				podConfig := pluginTimeoutPodConfig(ctx, rc, "nri-test-timeout-remove-", labelValue)
				podID := framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())
				Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())

				testStub.Plugin.OnRemovePodSandbox = func(_ context.Context, _ *nri.PodSandbox) error {
					return gate.block()
				}

				By(
					"removing the sandbox while the RemovePodSandbox hook blocks past the request timeout",
				)

				callCtx, cancel := context.WithTimeout(ctx, requestTimeout+pluginTimeoutCallSlack)
				defer cancel()

				removeErr := rc.RemovePodSandbox(callCtx, podID)
				callTimedOut := callCtx.Err() != nil

				gate.unblock()

				Expect(gate.wasReached()).To(BeTrue(), "RemovePodSandbox NRI hook was not invoked")
				Expect(callTimedOut).To(BeFalse(),
					"RemovePodSandbox did not return within %v; the runtime must not wait on the plugin "+
						"beyond its NRI request timeout (%v)", requestTimeout+pluginTimeoutCallSlack, requestTimeout)
				Expect(removeErr).NotTo(HaveOccurred(),
					"a RemovePodSandbox hook timeout MUST NOT prevent the sandbox from being removed")

				Expect(pluginTimeoutListSandboxes(ctx, rc, labelValue)).To(BeEmpty(),
					"sandbox MUST be removed")
			},
		)

		It(
			"should deliver StopPodSandbox to a plugin whose RunPodSandbox hook was slow but within the request timeout",
			func(ctx SpecContext) {
				var err error

				testStub, err = StartNRITestStub("cri-test-nri-timeout-slow-run", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				requestTimeout := pluginTimeoutRequestTimeout(testStub)

				// Half the request timeout is clearly slow, yet leaves the
				// other half as margin so the runtime does not time it out.
				slowDelay := requestTimeout / 2

				testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, _ *nri.PodSandbox) error {
					select {
					case <-time.After(slowDelay):
					case <-hookCtx.Done():
					}

					return nil
				}

				By("running a pod sandbox with a slow but in-time RunPodSandbox hook")

				podConfig := pluginTimeoutPodConfig(
					ctx,
					rc,
					"nri-test-timeout-slow-run-",
					labelValue,
				)
				start := time.Now()
				podID := framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())
				Expect(time.Since(start)).To(BeNumerically(">=", slowDelay),
					"RunPodSandbox should wait for the in-time hook to complete")

				By("stopping the sandbox")
				Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())

				By("verifying the plugin received StopPodSandbox for the sandbox")
				Eventually(func() []NRIEventType {
					events := FilterEventsByPodID(testStub.Plugin.Events(), podID)

					types := make([]NRIEventType, 0, len(events))
					for _, e := range events {
						types = append(types, e.Type)
					}

					return types
				}, 10*time.Second, 50*time.Millisecond).Should(ContainElements(
					EventRunPodSandbox,
					EventStopPodSandbox,
				), "a plugin whose RunPodSandbox hook completed in time MUST still receive StopPodSandbox")
			},
		)
	})
})
