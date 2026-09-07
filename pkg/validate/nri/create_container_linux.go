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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var (
		rc internalapi.RuntimeService
		ic internalapi.ImageManagerService
	)

	BeforeEach(func() {
		rc, ic = nriTestClients(f)
	})

	Context("CreateContainer contract", Serial, func() {
		var (
			testStub  *NRITestStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
			// containerID holds the successfully created retry container so
			// AfterEach can remove it even if an inline assertion fails.
			containerID string
			// joinInFlightCreate is set by specs that leave a CreateContainer
			// blocked in a goroutine. AfterEach calls it to release the hook,
			// join the goroutine and publish the resulting container ID, so an
			// aborted spec cannot leave a half-created container behind.
			joinInFlightCreate func()
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-create", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			// Ensure the test image is available before creating containers.
			framework.PullPublicImage(
				ctx,
				ic,
				framework.TestContext.TestImageList.DefaultTestContainerImage,
				nil,
			)

			By("creating a pod sandbox")

			podSandboxName := "nri-test-create-" + framework.NewUUID()
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
		})

		AfterEach(func(ctx SpecContext) {
			// Release and join any CreateContainer still blocked in a goroutine
			// before touching the stub, so the runtime is never finishing a
			// container creation while we tear the sandbox down.
			if joinInFlightCreate != nil {
				joinInFlightCreate()
			}

			// Stop the stub next so a still-failing hook cannot interfere with
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

			// Reset the Context-scoped state so the next spec never inherits an
			// already-removed ID or a stopped stub from this one.
			testStub, podID, containerID = nil, "", ""
			joinInFlightCreate = nil
		})

		It(
			"should not expose container while CreateContainer hook is in progress",
			func(ctx SpecContext) {
				// This test validates the spec contract: during CreateContainer hook
				// execution, the container MUST NOT be visible via ListContainers or
				// ContainerStatus.
				hookBlocking := make(chan struct{})
				hookReached := make(chan struct{})

				var hookContainerID string

				var hookOnce sync.Once

				// sync.Once guards against the hook being invoked more than
				// once, which would otherwise panic on a double close of
				// hookReached and race on hookContainerID.
				testStub.Plugin.SetOnCreateContainer(
					func(hookCtx context.Context, pod *nri.PodSandbox, container *nri.Container) error {
						// The plugin sees every container the runtime creates
						// while the stub is connected, including containers
						// created by other actors on the node (a kubelet, say).
						// hookContainerID is published for destructive cleanup
						// below, so only ever participate in the handshake for a
						// container going into this spec's own sandbox.
						if pod.GetId() != podID {
							return nil
						}

						firstInvocation := false

						hookOnce.Do(func() { firstInvocation = true })

						// Skip duplicate invocations so they are not blocked by the
						// test channel handshake.
						if !firstInvocation {
							return nil
						}

						hookContainerID = container.GetId()

						close(hookReached)

						select {
						case <-hookBlocking:
						case <-hookCtx.Done():
						}

						return nil
					},
				)

				By("triggering CreateContainer in a goroutine")

				containerName := "nri-test-block-create-" + framework.NewUUID()
				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						containerName,
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image:              framework.TestContext.TestImageList.DefaultTestContainerImage,
						UserSpecifiedImage: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				// Capture the sandbox ID before launching the goroutine. An
				// assertion failure between here and the Wait() below aborts the
				// spec without joining, so AfterEach can reset podID while this
				// goroutine is still reading it.
				sandboxID := podID

				var (
					createErr   error
					createdID   string
					createWg    sync.WaitGroup
					releaseOnce sync.Once
				)

				// releaseHook is idempotent so the timeout path, the success
				// path and AfterEach can all unblock the hook safely.
				releaseHook := func() { releaseOnce.Do(func() { close(hookBlocking) }) }

				createWg.Go(func() {
					createdID, createErr = rc.CreateContainer(
						ctx,
						sandboxID,
						containerConfig,
						podConfig,
					)
				})

				// An assertion failure below unwinds straight to AfterEach without
				// joining this goroutine, so hand AfterEach a way to release the
				// hook, wait for CreateContainer to return and publish whatever
				// container it produced for removal.
				joinInFlightCreate = func() {
					releaseHook()
					createWg.Wait()

					if containerID == "" && createdID != "" {
						containerID = createdID
					}
				}

				By("waiting for CreateContainer hook to be reached")

				select {
				case <-hookReached:
					// Hook is now blocking
				case <-time.After(30 * time.Second):
					releaseHook() // unblock to avoid goroutine leak
					Fail("Timed out waiting for CreateContainer NRI hook to fire")
				}

				// The runtime has already assigned the container its final ID by
				// the time the hook fires, so publish it for cleanup before any
				// assertion below can abort the spec: once the hook is released
				// the runtime completes the creation whether or not we get to
				// read the ID that CreateContainer returns.
				containerID = hookContainerID

				// Without a container ID from the hook the checks below would pass
				// vacuously, so fail loudly instead of silently verifying nothing.
				Expect(hookContainerID).NotTo(BeEmpty(),
					"CreateContainer hook MUST receive a container with a non-empty ID")

				By("verifying container is NOT listed while hook is blocking")

				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(listErr).NotTo(HaveOccurred())

				for _, c := range containers {
					Expect(c.GetId()).NotTo(Equal(hookContainerID),
						"Container %s MUST NOT be listed while CreateContainer hook is blocking", hookContainerID)
				}

				By("verifying ContainerStatus is not accessible while hook is blocking")

				blockedResp, blockedErr := rc.ContainerStatus(ctx, hookContainerID, false)
				if blockedErr == nil && blockedResp != nil && blockedResp.GetStatus() != nil {
					Expect(
						blockedResp.GetStatus().GetState(),
					).NotTo(Equal(runtimeapi.ContainerState_CONTAINER_CREATED),
						"Container MUST NOT report CREATED state while CreateContainer hook is in progress")
				}

				By("releasing the hook and verifying container is created")
				releaseHook()
				createWg.Wait()

				joinInFlightCreate = nil

				Expect(
					createErr,
				).NotTo(HaveOccurred(), "CreateContainer should succeed after hook returns")
				Expect(createdID).NotTo(BeEmpty())
				containerID = createdID

				// After hook completes, container should be in CREATED state
				statusResp, err := rc.ContainerStatus(ctx, containerID, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(
					statusResp.GetStatus().GetState(),
				).To(Equal(runtimeapi.ContainerState_CONTAINER_CREATED),
					"Container should be in CREATED state after CreateContainer hook completes")
			},
		)

		It(
			"should fail CreateContainer when the NRI hook errors, leak nothing, and allow retry",
			func(ctx SpecContext) {
				// Build the container config up front so the hook below can scope
				// itself to this spec's own container by name.
				containerName := "nri-test-create-error-ctr-" + framework.NewUUID()
				containerConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(
						containerName,
						framework.DefaultAttempt,
					),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				// Fail only the first CreateContainer invocation so the retry passes.
				var failOnce sync.Once

				testStub.Plugin.SetOnCreateContainer(
					func(_ context.Context, pod *nri.PodSandbox, container *nri.Container) error {
						// The plugin sees every container the runtime creates while
						// the stub is connected, including containers created by
						// other actors on the node (a kubelet, say). Only induce the
						// failure for this spec's own container: an unrelated
						// creation would otherwise consume failOnce and let the
						// CreateContainer below succeed, failing the assertion
						// before containerID is published for cleanup.
						if pod.GetId() != podID {
							return nil
						}

						// SPEC_DISCREPANCY: CRI-O does not populate the container
						// name in NRI CreateContainer metadata, so only reject on a
						// name that is present and belongs to another container.
						if name := container.GetName(); name != "" && name != containerName {
							return nil
						}

						shouldFail := false

						failOnce.Do(func() { shouldFail = true })

						if shouldFail {
							return errors.New("induced NRI CreateContainer failure")
						}

						return nil
					},
				)

				// Reset events so we only observe container events from this point.
				testStub.Plugin.Reset()

				By("attempting CreateContainer while the NRI hook is failing")

				failedContainerID, createErr := framework.CreateContainerWithError(
					ctx,
					rc,
					ic,
					containerConfig,
					podID,
					podConfig,
				)
				Expect(createErr).To(HaveOccurred(),
					"CreateContainer MUST fail when the NRI CreateContainer hook returns an error")
				Expect(failedContainerID).To(BeEmpty(),
					"No container ID should be returned when CreateContainer fails")

				By("verifying the NRI CreateContainer hook actually fired")
				// The hook records its event before returning the error, confirming
				// the failure was induced on the creation path as intended. Scope the
				// count to this spec's sandbox: the plugin also observes containers
				// created concurrently on the node, and an unrelated CreateContainer
				// would satisfy this assertion vacuously.
				Eventually(func() int {
					count := 0

					for _, e := range FilterEventsByPodID(testStub.Plugin.Events(), podID) {
						if e.Type == EventCreateContainer {
							count++
						}
					}

					return count
				}, 10*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 1),
					"NRI CreateContainer hook should have fired before the failure")

				By("verifying the failed CreateContainer leaked no container")

				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(listErr).NotTo(HaveOccurred(), "ListContainers after failed CreateContainer")

				for _, c := range containers {
					if c.GetMetadata() != nil && c.GetMetadata().GetName() == containerName {
						containerID = c.GetId() // hand to AfterEach for cleanup
						Fail(
							fmt.Sprintf(
								"container %s was leaked after a failed CreateContainer",
								c.GetId(),
							),
						)
					}
				}

				By("verifying the failed CreateContainer did not start a container")
				// A failed CreateContainer MUST NOT result in a started container.
				// The runtime may emit Stop/Remove events as part of internal cleanup
				// of the partially created container, but a StartContainer event must
				// never appear. Use Consistently so events delivered slightly after the
				// failure are still caught, and scope the count to this spec's sandbox
				// so a container started elsewhere on the node cannot fail the spec.
				Consistently(func() int {
					count := 0

					for _, e := range FilterEventsByPodID(testStub.Plugin.Events(), podID) {
						if e.Type == EventStartContainer {
							count++
						}
					}

					return count
				}, 2*time.Second, 200*time.Millisecond).Should(BeZero(),
					"a failed CreateContainer MUST NOT result in a started container")

				By("retrying CreateContainer after the NRI hook stops failing")

				retryName := "nri-test-create-retry-ctr-" + framework.NewUUID()
				retryConfig := &runtimeapi.ContainerConfig{
					Metadata: framework.BuildContainerMetadata(retryName, framework.DefaultAttempt),
					Image: &runtimeapi.ImageSpec{
						Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
					},
					Command: framework.DefaultPauseCommand,
					Linux:   &runtimeapi.LinuxContainerConfig{},
				}

				retryID := framework.CreateContainer(ctx, rc, ic, retryConfig, podID, podConfig)
				Expect(retryID).NotTo(BeEmpty(),
					"CreateContainer retry should succeed after the NRI hook stops failing")
				// Hand the retry container to AfterEach immediately so a failure in
				// the start/stop/remove assertions below cannot leak it.
				containerID = retryID

				By("verifying the retried container can be started, stopped, and removed")
				Expect(rc.StartContainer(ctx, retryID)).NotTo(HaveOccurred(),
					"the retried container should start successfully")
				Expect(rc.StopContainer(ctx, retryID, 0)).NotTo(HaveOccurred(),
					"the retried container should stop successfully")
				Expect(rc.RemoveContainer(ctx, retryID)).NotTo(HaveOccurred(),
					"the retried container should be removable")
				// Clear containerID so AfterEach does not attempt to stop/remove it again.
				containerID = ""
			},
		)
	})
})
