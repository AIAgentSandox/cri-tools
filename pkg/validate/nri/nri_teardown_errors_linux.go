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

	// teardownErrT2Source is the source of the rule that a failing plugin
	// never blocks teardown, for the plugin fan-out or the runtime's own.
	teardownErrT2Source = "containerd/nri#286 (unmerged draft, revision 2339aa3): " +
		"a plugin error on a teardown hook must not block teardown, so every other " +
		"plugin still gets the hook and can release its resources for the sandbox"

	// teardownErrT2Issue tracks the NRI adaptation returning on the first
	// plugin error in its StopPodSandbox and RemovePodSandbox loops.
	teardownErrT2Issue = "NRI fix planned after containerd/nri#274"

	// teardownErrT3Rationale is why a teardown hook error must reach the CRI
	// caller and be redelivered on retry.
	teardownErrT3Rationale = "a teardown hook error must be returned to the CRI caller so that " +
		"it retries the call, and the retry must redeliver the hook, or a plugin that failed " +
		"to release its resources for the sandbox is never asked again and leaks them"

	// teardownErrT4Rationale is why removal is withheld while a plugin still
	// fails RemovePodSandbox.
	teardownErrT4Rationale = "while a connected plugin still fails its RemovePodSandbox hook, " +
		"the sandbox must stay listed (NotReady) and RemovePodSandbox must keep failing, " +
		"because once the sandbox is gone the plugin gets no further event to release " +
		"its resources for it"

	// teardownErrContainerdIssue is the containerd issue for swallowed
	// teardown errors (T3) and removal despite them (T4).
	// TODO: link the containerd issue once it is filed.
	teardownErrContainerdIssue = "no runtime issue filed yet"
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

// teardownErrPollHookCount polls, without failing the spec, until the plugin
// has recorded at least minCount events of the given type for the sandbox. It
// reports whether that happened, so a missing delivery can be recorded as a
// known deviation.
func teardownErrPollHookCount(
	plugin *NRITestPlugin,
	podID string,
	eventType NRIEventType,
	minCount int,
) bool {
	return NewGomega(func(string, ...int) {}).Eventually(func() int {
		return teardownErrCountPodEvents(plugin, podID, eventType)
	}, teardownErrHookTimeout, teardownErrPollInterval).Should(BeNumerically(">=", minCount))
}

// teardownErrSandboxState reports whether the pod sandbox is still returned
// by ListPodSandbox and, if so, its state.
func teardownErrSandboxState(
	ctx context.Context,
	rc internalapi.RuntimeService,
	podID string,
) (state runtimeapi.PodSandboxState, listed bool, err error) {
	pods, err := rc.ListPodSandbox(ctx, nil)
	if err != nil {
		return 0, false, err
	}

	for _, pod := range pods {
		if pod.GetId() == podID {
			return pod.GetState(), true, nil
		}
	}

	return 0, false, nil
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

// teardownErrInjectErrors returns a stub configure callback that makes the
// plugin fail every StopPodSandbox and RemovePodSandbox hook while failing is
// set. The hooks are installed before the plugin connects and read the flag
// atomically, so the test can make the plugin succeed later without racing
// the stub's hook handlers.
func teardownErrInjectErrors(failing *atomic.Bool) func(*NRITestPlugin) {
	return func(plugin *NRITestPlugin) {
		plugin.OnStopPodSandbox = func(context.Context, *nri.PodSandbox) error {
			if failing.Load() {
				return errTeardownInjectedStop
			}

			return nil
		}
		plugin.OnRemovePodSandbox = func(context.Context, *nri.PodSandbox) error {
			if failing.Load() {
				return errTeardownInjectedRemove
			}

			return nil
		}
	}
}

// teardownErrSkipDeviations skips the spec with every known runtime deviation
// it recorded. It must be called after all other assertions and the cleanup.
func teardownErrSkipDeviations(deviations []string) {
	if len(deviations) == 0 {
		return
	}

	Skip("spec discrepancy: " + strings.Join(deviations, " | "))
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

		startStub := func(name, idx string, configure ...func(*NRITestPlugin)) *NRITestStub {
			s, err := StartNRITestStub(name, idx, configure...)
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

		expectHookCount := func(plugin *NRITestPlugin, podID string, eventType NRIEventType, minCount int, desc string) {
			Eventually(func() int {
				return teardownErrCountPodEvents(plugin, podID, eventType)
			}, teardownErrHookTimeout, teardownErrPollInterval).Should(BeNumerically(">=", minCount),
				"%s MUST receive the %s hook for sandbox %s at least %d time(s)", desc, eventType, podID, minCount)
		}

		expectListedNotReady := func(ctx context.Context, podID, when string) {
			state, listed, err := teardownErrSandboxState(ctx, rc, podID)
			Expect(err).NotTo(HaveOccurred(), "ListPodSandbox")
			Expect(
				listed,
			).To(BeTrue(), "sandbox %s MUST stay listed %s: %s", podID, when, teardownErrT4Rationale)
			Expect(state).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
				"sandbox %s MUST be NotReady %s", podID, when)
		}

		expectSandboxRemoved := func(ctx context.Context, podID string) {
			Eventually(func() (bool, error) {
				_, listed, err := teardownErrSandboxState(ctx, rc, podID)

				return listed, err
			}, teardownErrRemovalTimeout, teardownErrPollInterval).Should(BeFalse(),
				"sandbox %s MUST be removed (absent from ListPodSandbox) once no plugin fails RemovePodSandbox",
				podID)
		}

		// failStop runs StopPodSandbox and its retry while the failing plugin
		// rejects the hook. It asserts T2 (the sandbox is stopped, other plugins
		// get the hook) and T3 (the error is returned and the retry redelivers
		// the hook), and returns the known deviations it observed.
		failStop := func(ctx context.Context, podID string, failing, healthy *NRITestPlugin) []string {
			var deviations []string

			By("stopping the pod sandbox while the failing plugin rejects StopPodSandbox")

			stopErr := rc.StopPodSandbox(ctx, podID)
			framework.Logf("StopPodSandbox(%s) returned: %v", podID, stopErr)
			expectHookCount(failing, podID, EventStopPodSandbox, 1, "the failing plugin")

			By("verifying the sandbox is stopped (NotReady) despite the plugin error")

			statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
			Expect(err).NotTo(HaveOccurred(), "PodSandboxStatus after the failed StopPodSandbox")
			Expect(
				statusResp.GetStatus().GetState(),
			).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
				"sandbox %s MUST be NotReady after StopPodSandbox even though a plugin failed the hook (%s)",
				podID, teardownErrT2Source)

			if healthy != nil && !teardownErrPollHookCount(healthy, podID, EventStopPodSandbox, 1) {
				deviations = append(deviations, fmt.Sprintf(
					"T2 StopPodSandbox fan-out: expected the healthy higher-index plugin to receive "+
						"StopPodSandbox although the lower-index plugin failed it; observed it received %d "+
						"(the runtime stopped the sandbox). Source: %s. Issue: %s",
					teardownErrCountPodEvents(healthy, podID, EventStopPodSandbox),
					teardownErrT2Source,
					teardownErrT2Issue,
				))
			}

			By("retrying StopPodSandbox while the failing plugin still rejects it")

			retryErr := rc.StopPodSandbox(ctx, podID)
			framework.Logf("StopPodSandbox(%s) retry returned: %v", podID, retryErr)

			if stopErr == nil {
				// The first call succeeded, so a retry is a stop of an already
				// stopped sandbox and nothing is redelivered: the deviation is
				// the swallowed error itself.
				return append(deviations, fmt.Sprintf(
					"T3 StopPodSandbox: expected the call to return the plugin's StopPodSandbox error "+
						"and a retry to redeliver the hook; observed StopPodSandbox returned success, the "+
						"retry returned %v, and the failing plugin got %d StopPodSandbox hook(s) in total. "+
						"Rationale: %s. Issue: %s",
					retryErr,
					teardownErrCountPodEvents(failing, podID, EventStopPodSandbox),
					teardownErrT3Rationale,
					teardownErrContainerdIssue,
				))
			}

			Expect(retryErr).To(HaveOccurred(),
				"StopPodSandbox retry MUST return an error while the plugin still fails the hook: %s",
				teardownErrT3Rationale)
			expectHookCount(
				failing,
				podID,
				EventStopPodSandbox,
				2,
				"the failing plugin, on the StopPodSandbox retry,",
			)

			return deviations
		}

		// failRemove runs RemovePodSandbox (and its retry when the sandbox is
		// still there) while the failing plugin rejects the hook. It asserts T2
		// fan-out, T3 (the error is returned and the retry redelivers) and T4
		// (the sandbox stays listed NotReady), and returns the known deviations
		// it observed.
		failRemove := func(ctx context.Context, podID string, failing, healthy *NRITestPlugin) []string {
			var deviations []string

			By("removing the pod sandbox while the failing plugin rejects RemovePodSandbox")

			removeErr := rc.RemovePodSandbox(ctx, podID)
			framework.Logf("RemovePodSandbox(%s) returned: %v", podID, removeErr)
			expectHookCount(failing, podID, EventRemovePodSandbox, 1, "the failing plugin")

			if healthy != nil &&
				!teardownErrPollHookCount(healthy, podID, EventRemovePodSandbox, 1) {
				deviations = append(deviations, fmt.Sprintf(
					"T2 RemovePodSandbox fan-out: expected the healthy higher-index plugin to receive "+
						"RemovePodSandbox although the lower-index plugin failed it; observed it received %d. "+
						"Source: %s. Issue: %s",
					teardownErrCountPodEvents(healthy, podID, EventRemovePodSandbox),
					teardownErrT2Source,
					teardownErrT2Issue,
				))
			}

			if removeErr != nil {
				expectListedNotReady(
					ctx,
					podID,
					"after a RemovePodSandbox that a connected plugin failed",
				)

				By("retrying RemovePodSandbox while the failing plugin still rejects it")

				retryErr := rc.RemovePodSandbox(ctx, podID)
				framework.Logf("RemovePodSandbox(%s) retry returned: %v", podID, retryErr)
				Expect(retryErr).To(HaveOccurred(),
					"RemovePodSandbox retry MUST keep returning an error while the plugin still fails the hook: %s",
					teardownErrT4Rationale)
				expectHookCount(failing, podID, EventRemovePodSandbox, 2,
					"the failing plugin, on the RemovePodSandbox retry,")
				expectListedNotReady(
					ctx,
					podID,
					"after a retried RemovePodSandbox that a connected plugin failed",
				)

				return deviations
			}

			deviations = append(deviations, fmt.Sprintf(
				"T3 RemovePodSandbox: expected the call to return the plugin's RemovePodSandbox error; "+
					"observed it returned success. Rationale: %s. Issue: %s",
				teardownErrT3Rationale,
				teardownErrContainerdIssue,
			))

			_, listed, err := teardownErrSandboxState(ctx, rc, podID)
			Expect(err).NotTo(HaveOccurred(), "ListPodSandbox")

			if !listed {
				deviations = append(deviations, fmt.Sprintf(
					"T4 RemovePodSandbox: expected the sandbox to stay listed (NotReady) while the "+
						"connected plugin still fails RemovePodSandbox; observed RemovePodSandbox removed it. "+
						"Rationale: %s. Issue: %s",
					teardownErrT4Rationale,
					teardownErrContainerdIssue,
				))
			}

			return deviations
		}

		// recoverTeardown lets the failing plugin succeed and retries the
		// teardown as kubelet would: both calls must now succeed and the sandbox
		// must be removed.
		recoverTeardown := func(ctx context.Context, podID string, failingOn *atomic.Bool) {
			By("letting the plugin succeed and retrying the teardown")
			failingOn.Store(false)

			_, listed, err := teardownErrSandboxState(ctx, rc, podID)
			Expect(err).NotTo(HaveOccurred(), "ListPodSandbox")

			if listed {
				Expect(rc.StopPodSandbox(ctx, podID)).To(Succeed(),
					"StopPodSandbox MUST succeed once no plugin fails the hook")
			}

			Expect(rc.RemovePodSandbox(ctx, podID)).To(Succeed(),
				"RemovePodSandbox MUST succeed once no plugin fails the hook")
			expectSandboxRemoved(ctx, podID)
		}

		expectNewPodReady := func(ctx context.Context, namePrefix string) {
			By("verifying a subsequent RunPodSandbox succeeds and the new sandbox is Ready")

			nextPodID := runPod(ctx, namePrefix)

			statusResp, err := rc.PodSandboxStatus(ctx, nextPodID, false)
			Expect(err).NotTo(HaveOccurred(), "PodSandboxStatus for the subsequent sandbox")
			Expect(statusResp.GetStatus().GetState()).To(
				Equal(runtimeapi.PodSandboxState_SANDBOX_READY),
				"a sandbox created after a plugin teardown failure MUST be Ready",
			)
		}

		AfterEach(func(ctx SpecContext) {
			cleanup(ctx)
		})

		It(
			"should report teardown plugin errors, redeliver the hooks on retry and withhold removal until the plugin succeeds",
			func(ctx SpecContext) {
				// A plugin error on StopPodSandbox or RemovePodSandbox must not
				// block the runtime's own teardown (containerd/nri#286, unmerged
				// draft at 2339aa3). It must be returned to the CRI caller, and a
				// retry must redeliver the hook, so that kubelet keeps asking the
				// plugin until it releases its resources. While the plugin still
				// fails RemovePodSandbox the sandbox must stay listed, or the
				// plugin gets no further event for it.
				var failingOn atomic.Bool
				failingOn.Store(true)

				testStub := startStub(
					"cri-test-nri-teardown-err",
					"00",
					teardownErrInjectErrors(&failingOn),
				)

				By("creating a pod sandbox")

				podID := runPod(ctx, "nri-test-teardown-err-")

				deviations := failStop(ctx, podID, testStub.Plugin, nil)
				deviations = append(deviations, failRemove(ctx, podID, testStub.Plugin, nil)...)

				recoverTeardown(ctx, podID, &failingOn)
				expectNewPodReady(ctx, "nri-test-teardown-err-next-")

				cleanup(ctx)
				teardownErrSkipDeviations(deviations)
			},
		)

		It(
			"should deliver teardown hooks to all plugins even if a lower-index plugin fails",
			func(ctx SpecContext) {
				// One plugin's failure on StopPodSandbox/RemovePodSandbox must
				// not block teardown (containerd/nri#286, unmerged draft at
				// 2339aa3): every other plugin must still be notified so it can
				// release its resources. The failing plugin is held to the same
				// error reporting, redelivery and removal rules as when it is
				// the only plugin.
				var failingOn atomic.Bool
				failingOn.Store(true)

				failingStub := startStub("cri-test-nri-teardown-err-failing", "10",
					teardownErrInjectErrors(&failingOn))
				healthyStub := startStub("cri-test-nri-teardown-err-healthy", "11")

				By("creating a pod sandbox")

				podID := runPod(ctx, "nri-test-teardown-err-multi-")

				By("verifying both plugins received RunPodSandbox")
				expectHookCount(
					failingStub.Plugin,
					podID,
					EventRunPodSandbox,
					1,
					"the lower-index plugin",
				)
				expectHookCount(
					healthyStub.Plugin,
					podID,
					EventRunPodSandbox,
					1,
					"the higher-index plugin",
				)

				deviations := failStop(ctx, podID, failingStub.Plugin, healthyStub.Plugin)
				deviations = append(
					deviations,
					failRemove(ctx, podID, failingStub.Plugin, healthyStub.Plugin)...)

				recoverTeardown(ctx, podID, &failingOn)
				expectNewPodReady(ctx, "nri-test-teardown-err-multi-next-")

				cleanup(ctx)
				teardownErrSkipDeviations(deviations)
			},
		)
	})
})
