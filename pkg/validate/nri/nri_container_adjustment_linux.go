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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
	adjustEnvKey        = "NRI_ADJUST_TEST_ENV"
	adjustEnvValue      = "injected-by-nri"
	adjustAnnotationKey = "cri-tools.test/nri-adjust"
	adjustAnnotationVal = "injected-by-nri"
	adjustMountPath     = "/nri-adjust-tmpfs"

	// adjustMemoryLimit is the memory limit (64 MiB) applied by the plugin.
	adjustMemoryLimit int64 = 64 * 1024 * 1024

	adjustExecTimeout = 10 * time.Second
)

// adjustedContainerIDs returns the IDs of the containers for which the
// plugin's CreateContainer hook has fired, which is where the adjustment under
// test is returned to the runtime.
func adjustedContainerIDs(plugin *NRITestPlugin) []string {
	var ids []string

	for _, e := range plugin.Events() {
		if e.Type == EventCreateContainer {
			ids = append(ids, e.ContainerID)
		}
	}

	return ids
}

// adjustExec runs cmd in the container and returns its trimmed stdout.
func adjustExec(
	ctx context.Context,
	rc internalapi.RuntimeService,
	containerID string,
	cmd ...string,
) string {
	stdout, stderr, err := rc.ExecSync(ctx, containerID, cmd, adjustExecTimeout)
	Expect(err).NotTo(HaveOccurred(),
		"ExecSync %v failed: stdout=%q stderr=%q", cmd, string(stdout), string(stderr))

	return strings.TrimSpace(string(stdout))
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

	Context("container adjustment", Serial, func() {
		var (
			testStub    *NRITestStub
			podID       string
			podConfig   *runtimeapi.PodSandboxConfig
			containerID string
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-adjust", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			framework.PullPublicImage(
				ctx,
				ic,
				framework.TestContext.TestImageList.DefaultTestContainerImage,
				nil,
			)

			By("creating a pod sandbox")

			podSandboxName := "nri-test-adjust-" + framework.NewUUID()
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
			// Disconnect the plugin first so its adjustments cannot affect teardown.
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

				containerID = ""
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

		newContainerConfig := func(prefix string) *runtimeapi.ContainerConfig {
			return &runtimeapi.ContainerConfig{
				Metadata: framework.BuildContainerMetadata(
					prefix+framework.NewUUID(),
					framework.DefaultAttempt,
				),
				Image: &runtimeapi.ImageSpec{
					Image: framework.TestContext.TestImageList.DefaultTestContainerImage,
				},
				Command: framework.DefaultPauseCommand,
				Linux:   &runtimeapi.LinuxContainerConfig{},
			}
		}

		createAndStart := func(ctx context.Context, config *runtimeapi.ContainerConfig) {
			containerID = framework.CreateContainer(ctx, rc, ic, config, podID, podConfig)

			Expect(adjustedContainerIDs(testStub.Plugin)).To(ContainElement(containerID),
				"NRI CreateContainer hook should have been invoked for the container")

			By("starting the container")
			Expect(rc.StartContainer(ctx, containerID)).To(Succeed())
		}

		It(
			"should apply environment variable and mount adjustments to the container",
			func(ctx SpecContext) {
				testStub.Plugin.OnCreateContainer = func(
					_ context.Context,
					_ *nri.PodSandbox,
					_ *nri.Container,
				) (*nri.ContainerAdjustment, error) {
					adj := &nri.ContainerAdjustment{}
					adj.AddEnv(adjustEnvKey, adjustEnvValue)
					adj.AddMount(&nri.Mount{
						Destination: adjustMountPath,
						Type:        "tmpfs",
						Source:      "tmpfs",
						Options:     []string{"nosuid", "nodev", "size=1m"},
					})

					return adj, nil
				}

				By("creating a container adjusted by the NRI plugin")
				createAndStart(ctx, newContainerConfig("nri-test-adjust-env-mount-"))

				By("verifying the injected environment variable is visible to exec'd processes")
				Expect(adjustExec(ctx, rc, containerID, "printenv", adjustEnvKey)).
					To(Equal(adjustEnvValue))

				By("verifying the injected tmpfs mount is present in the container")

				mounts := adjustExec(ctx, rc, containerID, "cat", "/proc/self/mounts")

				var mountLine string

				for line := range strings.SplitSeq(mounts, "\n") {
					fields := strings.Fields(line)
					if len(fields) >= 3 && fields[1] == adjustMountPath {
						mountLine = line
					}
				}

				Expect(mountLine).NotTo(BeEmpty(),
					"mount %s injected by NRI should be present in /proc/self/mounts:\n%s",
					adjustMountPath, mounts)
				Expect(strings.Fields(mountLine)[2]).To(Equal("tmpfs"),
					"mount %s injected by NRI should be a tmpfs", adjustMountPath)

				By("verifying the injected mount carries the requested options")
				// The kernel reports the tmpfs size in KiB: size=1m reads back as size=1024k.
				Expect(strings.Fields(mountLine)).To(HaveLen(6),
					"unexpected /proc/self/mounts line %q", mountLine)
				Expect(strings.Split(strings.Fields(mountLine)[3], ",")).To(
					ContainElements("nosuid", "nodev", "size=1024k"),
					"mount %s injected by NRI should carry the options nosuid, nodev and size=1m: %q",
					adjustMountPath, mountLine)

				By("verifying the injected mount is writable")
				adjustExec(ctx, rc, containerID, "touch", adjustMountPath+"/probe")
			},
		)

		It(
			"should expose an annotation adjustment in the OCI runtime spec of verbose ContainerStatus",
			func(ctx SpecContext) {
				testStub.Plugin.OnCreateContainer = func(
					_ context.Context,
					_ *nri.PodSandbox,
					_ *nri.Container,
				) (*nri.ContainerAdjustment, error) {
					adj := &nri.ContainerAdjustment{}
					adj.AddAnnotation(adjustAnnotationKey, adjustAnnotationVal)

					return adj, nil
				}

				By("creating a container adjusted by the NRI plugin")
				createAndStart(ctx, newContainerConfig("nri-test-adjust-annotation-"))

				By("reading the OCI runtime spec from verbose ContainerStatus info")
				// NRI adjusts the OCI spec, not the CRI ContainerConfig. The CRI API
				// requires ContainerStatus.Annotations to equal the annotations of the
				// ContainerConfig, so the adjustment is only observable in the OCI
				// runtime spec that runtimes such as containerd and CRI-O report under
				// the "info" key of the verbose status.
				resp, err := rc.ContainerStatus(ctx, containerID, true)
				Expect(err).NotTo(HaveOccurred())

				var info struct {
					RuntimeSpec *struct {
						Annotations map[string]string `json:"annotations"`
					} `json:"runtimeSpec"`
				}

				rawInfo, ok := resp.GetInfo()["info"]
				if !ok || json.Unmarshal([]byte(rawInfo), &info) != nil || info.RuntimeSpec == nil {
					Skip("The runtime does not report the OCI runtime spec as " +
						`info["info"].runtimeSpec in verbose ContainerStatus, so an annotation ` +
						"added by an NRI CreateContainer adjustment cannot be observed through CRI.")
				}

				Expect(info.RuntimeSpec.Annotations).To(
					HaveKeyWithValue(adjustAnnotationKey, adjustAnnotationVal),
					"OCI runtime spec reported by verbose ContainerStatus must carry the "+
						"annotation added by the NRI adjustment")
			},
		)

		It(
			"should apply a memory limit adjustment to the container cgroup",
			func(ctx SpecContext) {
				testStub.Plugin.OnCreateContainer = func(
					_ context.Context,
					_ *nri.PodSandbox,
					_ *nri.Container,
				) (*nri.ContainerAdjustment, error) {
					adj := &nri.ContainerAdjustment{}
					adj.SetLinuxMemoryLimit(adjustMemoryLimit)

					return adj, nil
				}

				By("creating a container adjusted by the NRI plugin")
				createAndStart(ctx, newContainerConfig("nri-test-adjust-memory-"))

				By("reading the memory limit from the container cgroup")
				// cgroup v2 exposes memory.max; cgroup v1 exposes memory.limit_in_bytes.
				out := adjustExec(ctx, rc, containerID, "sh", "-c",
					"cat /sys/fs/cgroup/memory.max 2>/dev/null || "+
						"cat /sys/fs/cgroup/memory/memory.limit_in_bytes")

				limit, err := strconv.ParseInt(out, 10, 64)
				Expect(err).NotTo(HaveOccurred(), "unexpected memory limit %q", out)
				Expect(limit).To(Equal(adjustMemoryLimit),
					"container cgroup memory limit should match the NRI adjustment")
			},
		)

		It(
			"should reject a seccomp adjustment for a RuntimeDefault container when the default validator is enabled",
			func(ctx SpecContext) {
				testStub.Plugin.OnCreateContainer = func(
					_ context.Context,
					_ *nri.PodSandbox,
					_ *nri.Container,
				) (*nri.ContainerAdjustment, error) {
					adj := &nri.ContainerAdjustment{}
					adj.SetLinuxSeccompPolicy(
						&nri.LinuxSeccomp{DefaultAction: "SCMP_ACT_ALLOW"},
					)

					return adj, nil
				}

				config := newContainerConfig("nri-test-adjust-seccomp-")
				config.Linux.SecurityContext = &runtimeapi.LinuxContainerSecurityContext{
					Seccomp: &runtimeapi.SecurityProfile{
						ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
					},
				}

				// Whether the validator rejects this adjustment is runtime
				// configuration that CRI does not report, so the operator states it
				// with a flag. It is never inferred from the outcome under test.
				if !framework.TestContext.NRIValidatorRejectsSeccomp {
					Skip("The NRI default validator is not known to reject seccomp adjustments " +
						"of RuntimeDefault containers on this runtime. Enable it in the runtime " +
						"(containerd: reject_runtime_default_seccomp_adjustment, see " +
						"https://github.com/containerd/nri#default-validation) and pass " +
						"--nri-validator-rejects-seccomp to run this test.")
				}

				By(
					"creating a RuntimeDefault seccomp container whose policy the NRI plugin overrides",
				)

				createdID, err := framework.CreateContainerWithError(
					ctx, rc, ic, config, podID, podConfig)
				if err == nil {
					containerID = createdID // hand to AfterEach for cleanup

					Fail("CreateContainer succeeded although the NRI default validator is " +
						"configured to reject an adjustment that replaces the RuntimeDefault " +
						"seccomp policy requested by the container security context")
				}

				framework.Logf("CreateContainer rejected the seccomp adjustment: %v", err)
				// The NRI API formats a validator rejection as
				// `validator "<name>" rejected container adjustment, reason: ...`. Any
				// other error is a plugin or runtime failure, not the validator's.
				Expect(err.Error()).To(ContainSubstring("rejected container adjustment"),
					"CreateContainer must fail because the NRI validator rejected the adjustment")
				Expect(createdID).To(BeEmpty(),
					"no container ID should be returned when the adjustment is rejected")

				By("verifying the rejected container is not listed when CreateContainer returns")

				containers, err := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(err).NotTo(HaveOccurred())

				for _, c := range containers {
					if c.GetMetadata().GetName() == config.GetMetadata().GetName() {
						containerID = c.GetId() // hand to AfterEach for cleanup
						Fail(fmt.Sprintf(
							"container %s is still listed after the adjustment was rejected",
							c.GetId(),
						))
					}
				}

				By("verifying the plugin was told to remove the rejected container")
				// A plugin that allocated on CreateContainer must be told when the
				// container is rolled back, or it leaks: it gets RemoveContainer
				// before the CRI call returns, after its CreateContainer.
				events := testStub.Plugin.Events()
				createIdx, removeIdx := -1, -1

				for i, e := range events {
					if e.ContainerName != config.GetMetadata().GetName() {
						continue
					}

					switch {
					case e.Type == EventCreateContainer && createIdx < 0:
						createIdx = i
					case e.Type == EventRemoveContainer && createIdx >= 0 &&
						e.ContainerID == events[createIdx].ContainerID && removeIdx < 0:
						removeIdx = i
					}
				}

				Expect(createIdx).To(BeNumerically(">=", 0),
					"NRI CreateContainer hook should have been invoked before the rejection")
				Expect(removeIdx).To(BeNumerically(">", createIdx),
					"plugin must receive RemoveContainer for the rejected container %s "+
						"after its CreateContainer, before CreateContainer returns; events: %+v",
					events[createIdx].ContainerID, events)

				By("retrying the create with the same metadata and no adjustment")
				// The rejected create must leave nothing behind that blocks the name.
				testStub.Plugin.OnCreateContainer = nil
				containerID = framework.CreateContainer(ctx, rc, ic, config, podID, podConfig)
				Expect(containerID).NotTo(BeEmpty())
			},
		)
	})
})
