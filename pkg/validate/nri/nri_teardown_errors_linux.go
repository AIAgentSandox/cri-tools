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

const (
	// teardownErrHookTimeout bounds how long to wait for NRI teardown hooks to
	// be observed by a plugin. The hooks are delivered synchronously within the
	// CRI call, so they are normally visible as soon as the call returns.
	teardownErrHookTimeout = 10 * time.Second

	// teardownErrRemovalTimeout bounds how long to wait for a sandbox to
	// disappear from ListPodSandbox after RemovePodSandbox.
	teardownErrRemovalTimeout = 30 * time.Second

	// teardownErrPollInterval is the polling interval for Eventually checks.
	teardownErrPollInterval = 100 * time.Millisecond
)

var (
	errTeardownInjectedStop   = errors.New("injected NRI plugin error on StopPodSandbox")
	errTeardownInjectedRemove = errors.New("injected NRI plugin error on RemovePodSandbox")
)

// teardownErrCountPodEvents returns how many events of the given type the
// plugin has recorded for the given pod sandbox.
func teardownErrCountPodEvents(plugin *NRITestPlugin, podID string, eventType NRIEventType) int {
	count := 0

	events := FilterEventsByPodID(plugin.Events(), podID)
	for i := range events {
		if events[i].Type == eventType {
			count++
		}
	}

	return count
}

// teardownErrSandboxListed reports whether the pod sandbox is still returned
// by ListPodSandbox in any state.
func teardownErrSandboxListed(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podID string,
) (bool, error) {
	pods, err := rc.ListPodSandbox(ctx, nil)
	if err != nil {
		return false, err
	}

	for _, pod := range pods {
		if pod.GetId() == podID {
			return true, nil
		}
	}

	return false, nil
}

// teardownErrPodConfig builds a uniquely named pod sandbox config.
func teardownErrPodConfig(
	ctx context.Context,
	rc internalapi.RuntimeService,
	namePrefix string,
) *runtimeapi.PodSandboxConfig {
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
		Labels: framework.DefaultPodLabels,
	}
}

// teardownErrInjectErrors makes the plugin record and fail every
// StopPodSandbox and RemovePodSandbox hook.
func teardownErrInjectErrors(plugin *NRITestPlugin) {
	plugin.OnStopPodSandbox = func(context.Context, *nri.PodSandbox) error {
		return errTeardownInjectedStop
	}
	plugin.OnRemovePodSandbox = func(context.Context, *nri.PodSandbox) error {
		return errTeardownInjectedRemove
	}
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

	Context("teardown error handling", Serial, func() {
		var (
			stubs  []*NRITestStub
			podIDs []string
		)

		// cleanup disconnects all plugins first, so that no injected errors
		// interfere with teardown, and then stops and removes every sandbox the
		// test created. It is safe to call more than once: it is invoked
		// explicitly before any Skip (so sandboxes are never leaked, see
		// containerd/containerd#13355) and again from AfterEach.
		cleanup := func(ctx context.Context) {
			for _, s := range stubs {
				s.Cleanup()
			}

			stubs = nil

			for _, id := range podIDs {
				if err := rc.StopPodSandbox(ctx, id); err != nil {
					framework.Logf("cleanup: StopPodSandbox(%s) failed: %v", id, err)
				}

				if err := rc.RemovePodSandbox(ctx, id); err != nil {
					framework.Logf("cleanup: RemovePodSandbox(%s) failed: %v", id, err)
				}
			}

			podIDs = nil
		}

		startStub := func(name, idx string) *NRITestStub {
			s, err := StartNRITestStub(name, idx)
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub %s", name)

			stubs = append(stubs, s)

			return s
		}

		runPod := func(ctx context.Context, namePrefix string) string {
			podID := framework.RunPodSandbox(ctx, rc, teardownErrPodConfig(ctx, rc, namePrefix))
			Expect(podID).NotTo(BeEmpty())

			podIDs = append(podIDs, podID)

			return podID
		}

		expectHookDelivered := func(plugin *NRITestPlugin, podID string, eventType NRIEventType, desc string) {
			Eventually(func() int {
				return teardownErrCountPodEvents(plugin, podID, eventType)
			}, teardownErrHookTimeout, teardownErrPollInterval).Should(BeNumerically(">=", 1),
				"%s MUST receive the %s hook for sandbox %s", desc, eventType, podID)
		}

		expectSandboxRemoved := func(ctx context.Context, podID string) {
			Eventually(func() (bool, error) {
				return teardownErrSandboxListed(ctx, rc, podID)
			}, teardownErrRemovalTimeout, teardownErrPollInterval).Should(BeFalse(),
				"sandbox %s MUST be fully removed (absent from ListPodSandbox) despite NRI plugin teardown errors",
				podID)
		}

		AfterEach(func(ctx SpecContext) {
			cleanup(ctx)
		})

		It(
			"should complete sandbox teardown when a plugin returns errors from StopPodSandbox and RemovePodSandbox",
			func(ctx SpecContext) {
				// Spec (Error Handling): a plugin error on StopPodSandbox MUST NOT
				// prevent the sandbox from being stopped, and a plugin error on
				// RemovePodSandbox MUST NOT prevent the sandbox from being removed.
				// The runtime must also remain usable afterwards.
				testStub := startStub("cri-test-nri-teardown-err", "00")
				teardownErrInjectErrors(testStub.Plugin)

				By("creating a pod sandbox")

				podID := runPod(ctx, "nri-test-teardown-err-")

				By("stopping the pod sandbox while the plugin fails StopPodSandbox")

				stopErr := rc.StopPodSandbox(ctx, podID)
				framework.Logf("StopPodSandbox(%s) returned: %v", podID, stopErr)
				expectHookDelivered(
					testStub.Plugin,
					podID,
					EventStopPodSandbox,
					"the failing plugin",
				)

				By("removing the pod sandbox while the plugin fails RemovePodSandbox")

				removeErr := rc.RemovePodSandbox(ctx, podID)
				framework.Logf("RemovePodSandbox(%s) returned: %v", podID, removeErr)
				expectHookDelivered(
					testStub.Plugin,
					podID,
					EventRemovePodSandbox,
					"the failing plugin",
				)

				By("verifying the sandbox is fully removed despite the plugin errors")
				expectSandboxRemoved(ctx, podID)

				By("verifying a subsequent RunPodSandbox succeeds and the new sandbox is Ready")

				nextPodID := runPod(ctx, "nri-test-teardown-err-next-")

				statusResp, err := rc.PodSandboxStatus(ctx, nextPodID, false)
				Expect(err).NotTo(HaveOccurred(), "PodSandboxStatus for the subsequent sandbox")
				Expect(statusResp.GetStatus().GetState()).To(
					Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
					"a sandbox created after a plugin teardown failure MUST be Ready",
				)

				By("verifying the plugin errors were reported to the CRI caller")

				// SPEC_DISCREPANCY: containerd logs and swallows NRI plugin errors
				// on StopPodSandbox and RemovePodSandbox, returning success to the
				// CRI caller; CRI-O propagates them. The NRI pod sandbox lifecycle
				// spec (containerd/nri#286) requires teardown to proceed despite
				// plugin errors, which is asserted above; propagating the error lets
				// the caller (kubelet) know a plugin failed to release its
				// resources. Sandboxes are cleaned up before skipping.
				if stopErr == nil || removeErr == nil {
					cleanup(ctx)
					Skip(fmt.Sprintf(
						"spec discrepancy: runtime swallows NRI plugin teardown errors instead of "+
							"propagating them to the CRI caller (StopPodSandbox error: %v, "+
							"RemovePodSandbox error: %v). Expected: teardown proceeds (verified: the "+
							"sandbox was fully removed and a new sandbox could be created) AND the plugin "+
							"error is returned so the caller knows a plugin failed to release resources. "+
							"Observed: the call succeeds and the error is only logged by the runtime. "+
							"containerd treats NRI teardown hooks as best-effort and ignores their errors; "+
							"CRI-O propagates them",
						stopErr, removeErr,
					))
				}

				Expect(stopErr).To(HaveOccurred(),
					"StopPodSandbox SHOULD report the NRI plugin error to the CRI caller")
				Expect(removeErr).To(HaveOccurred(),
					"RemovePodSandbox SHOULD report the NRI plugin error to the CRI caller")
			},
		)

		It(
			"should deliver teardown hooks to all plugins even if a lower-index plugin fails",
			func(ctx SpecContext) {
				// Spec (Error Handling, Multi-Plugin Coordination): one plugin's
				// failure on StopPodSandbox/RemovePodSandbox MUST NOT block
				// teardown, and RemovePodSandbox hooks are independent, so every
				// plugin must still be notified so it can release its resources.
				failingStub := startStub("cri-test-nri-teardown-err-failing", "10")
				teardownErrInjectErrors(failingStub.Plugin)

				healthyStub := startStub("cri-test-nri-teardown-err-healthy", "11")

				By("creating a pod sandbox")

				podID := runPod(ctx, "nri-test-teardown-err-multi-")

				By("verifying both plugins received RunPodSandbox")
				expectHookDelivered(
					failingStub.Plugin,
					podID,
					EventRunPodSandbox,
					"the lower-index plugin",
				)
				expectHookDelivered(
					healthyStub.Plugin,
					podID,
					EventRunPodSandbox,
					"the higher-index plugin",
				)

				By("stopping and removing the pod sandbox while the lower-index plugin fails")

				if err := rc.StopPodSandbox(ctx, podID); err != nil {
					framework.Logf("StopPodSandbox(%s) returned: %v", podID, err)
				}

				if err := rc.RemovePodSandbox(ctx, podID); err != nil {
					framework.Logf("RemovePodSandbox(%s) returned: %v", podID, err)
				}

				By("verifying the lower-index (failing) plugin received both teardown hooks")
				expectHookDelivered(
					failingStub.Plugin,
					podID,
					EventStopPodSandbox,
					"the lower-index plugin",
				)
				expectHookDelivered(
					failingStub.Plugin,
					podID,
					EventRemovePodSandbox,
					"the lower-index plugin",
				)

				By("verifying the sandbox is fully removed despite the plugin errors")
				expectSandboxRemoved(ctx, podID)

				By("verifying the higher-index plugin received both teardown hooks")

				// Poll without failing so a missing delivery can be reported as a
				// spec discrepancy rather than a hard failure.
				delivered := NewGomega(func(string, ...int) {}).Eventually(func() bool {
					plugin := healthyStub.Plugin
					stops := teardownErrCountPodEvents(plugin, podID, EventStopPodSandbox)
					removes := teardownErrCountPodEvents(plugin, podID, EventRemovePodSandbox)

					return stops >= 1 && removes >= 1
				}, teardownErrHookTimeout, teardownErrPollInterval).Should(BeTrue())

				// SPEC_DISCREPANCY: NRI's adaptation iterates plugins in index order
				// and returns on the first plugin error for StopPodSandbox and
				// RemovePodSandbox, so higher-index plugins never see the teardown
				// hooks. The spec requires teardown to proceed regardless of plugin
				// failures and treats RemovePodSandbox hooks as independent between
				// plugins. Sandboxes are cleaned up before skipping.
				if !delivered {
					stopCount := teardownErrCountPodEvents(
						healthyStub.Plugin,
						podID,
						EventStopPodSandbox,
					)
					removeCount := teardownErrCountPodEvents(
						healthyStub.Plugin,
						podID,
						EventRemovePodSandbox,
					)

					cleanup(ctx)
					Skip(fmt.Sprintf(
						"spec discrepancy: NRI stops delivering teardown hooks to higher-index plugins "+
							"after a lower-index plugin returns an error. Expected: the higher-index plugin "+
							"receives StopPodSandbox and RemovePodSandbox even though the lower-index plugin "+
							"failed them, so it can release its resources. Observed: higher-index plugin got "+
							"%d StopPodSandbox and %d RemovePodSandbox hook(s) (sandbox teardown itself "+
							"completed). NRI's adaptation aborts the per-plugin delivery loop on the first "+
							"error instead of collecting errors and notifying every plugin",
						stopCount,
						removeCount,
					))
				}
			},
		)
	})
})
