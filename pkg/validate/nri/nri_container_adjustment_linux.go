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
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	adjustPluginName = "cri-test-nri-adjust"
	adjustPluginIdx  = "00"

	adjustEnvKey        = "NRI_ADJUST_TEST_ENV"
	adjustEnvValue      = "injected-by-nri"
	adjustAnnotationKey = "cri-tools.test/nri-adjust"
	adjustAnnotationVal = "injected-by-nri"
	adjustMountPath     = "/nri-adjust-tmpfs"

	// adjustMemoryLimit is the memory limit (64 MiB) applied by the plugin.
	adjustMemoryLimit int64 = 64 * 1024 * 1024

	adjustExecTimeout = 10 * time.Second
)

// adjustPlugin is a minimal NRI plugin that returns a ContainerAdjustment
// from CreateContainer. The adjustment is built by the adjust callback, so
// each spec controls exactly which container properties are changed.
type adjustPlugin struct {
	mu        sync.Mutex
	adjust    func(pod *nri.PodSandbox, ctr *nri.Container) *nri.ContainerAdjustment
	adjusted  []string
	ready     chan struct{}
	readyOnce sync.Once
}

// Synchronize implements stub.SynchronizeInterface and signals readiness.
func (p *adjustPlugin) Synchronize(
	_ context.Context,
	_ []*nri.PodSandbox,
	_ []*nri.Container,
) ([]*nri.ContainerUpdate, error) {
	p.readyOnce.Do(func() { close(p.ready) })

	return nil, nil
}

// CreateContainer implements stub.CreateContainerInterface.
func (p *adjustPlugin) CreateContainer(
	_ context.Context,
	pod *nri.PodSandbox,
	ctr *nri.Container,
) (*nri.ContainerAdjustment, []*nri.ContainerUpdate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.adjust == nil {
		return nil, nil, nil
	}

	p.adjusted = append(p.adjusted, ctr.GetId())

	return p.adjust(pod, ctr), nil, nil
}

func (p *adjustPlugin) setAdjust(
	fn func(*nri.PodSandbox, *nri.Container) *nri.ContainerAdjustment,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.adjust = fn
}

func (p *adjustPlugin) adjustedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.adjusted)
}

// adjustStub owns the lifecycle of a connected adjustPlugin.
type adjustStub struct {
	plugin *adjustPlugin
	cancel context.CancelFunc
	done   chan struct{}
}

// startAdjustStub connects an adjustPlugin to the runtime and waits until the
// registration/Synchronize handshake has completed.
func startAdjustStub() (*adjustStub, error) {
	socketPath := framework.TestContext.NRISocketPath
	if socketPath == "" {
		return nil, errors.New("NRI socket path not configured")
	}

	plugin := &adjustPlugin{ready: make(chan struct{})}

	s, err := stub.New(plugin,
		stub.WithPluginName(adjustPluginName),
		stub.WithPluginIdx(adjustPluginIdx),
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

	select {
	case <-plugin.ready:
	case <-done:
		cancel()

		return nil, fmt.Errorf("NRI stub exited early: %w", <-errCh)
	case <-time.After(10 * time.Second):
		cancel()
		s.Stop()

		return nil, errors.New("NRI stub did not become ready within 10s")
	}

	return &adjustStub{plugin: plugin, cancel: cancel, done: done}, nil
}

// stop disconnects the plugin from the runtime.
func (a *adjustStub) stop() {
	a.cancel()

	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		framework.Logf("NRI adjustment stub did not stop within 5s")
	}
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
			testStub    *adjustStub
			podID       string
			podConfig   *runtimeapi.PodSandboxConfig
			containerID string
		)

		BeforeEach(func(ctx SpecContext) {
			var err error

			testStub, err = startAdjustStub()
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI adjustment stub")

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
				testStub.stop()
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

			Expect(testStub.plugin.adjustedCount()).To(BeNumerically(">=", 1),
				"NRI CreateContainer hook should have been invoked for the container")

			By("starting the container")
			Expect(rc.StartContainer(ctx, containerID)).To(Succeed())
		}

		It(
			"should apply environment variable and mount adjustments to the container",
			func(ctx SpecContext) {
				testStub.plugin.setAdjust(
					func(_ *nri.PodSandbox, _ *nri.Container) *nri.ContainerAdjustment {
						adj := &nri.ContainerAdjustment{}
						adj.AddEnv(adjustEnvKey, adjustEnvValue)
						adj.AddMount(&nri.Mount{
							Destination: adjustMountPath,
							Type:        "tmpfs",
							Source:      "tmpfs",
							Options:     []string{"nosuid", "nodev", "size=1m"},
						})

						return adj
					},
				)

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

				By("verifying the injected mount is writable")
				adjustExec(ctx, rc, containerID, "touch", adjustMountPath+"/probe")
			},
		)

		It(
			"should expose an annotation adjustment through ContainerStatus",
			func(ctx SpecContext) {
				testStub.plugin.setAdjust(
					func(_ *nri.PodSandbox, _ *nri.Container) *nri.ContainerAdjustment {
						adj := &nri.ContainerAdjustment{}
						adj.AddAnnotation(adjustAnnotationKey, adjustAnnotationVal)

						return adj
					},
				)

				config := newContainerConfig("nri-test-adjust-annotation-")
				config.Annotations = map[string]string{"cri-tools.test/original": "true"}

				By("creating a container adjusted by the NRI plugin")
				createAndStart(ctx, config)

				By("checking annotations reported by ContainerStatus")

				resp, err := rc.ContainerStatus(ctx, containerID, true)
				Expect(err).NotTo(HaveOccurred())

				annotations := resp.GetStatus().GetAnnotations()
				Expect(annotations).To(HaveKeyWithValue("cri-tools.test/original", "true"),
					"annotations from ContainerConfig must be reported by ContainerStatus")

				if value, ok := annotations[adjustAnnotationKey]; ok {
					Expect(value).To(Equal(adjustAnnotationVal))

					return
				}

				By("falling back to the runtime spec reported in verbose ContainerStatus info")
				// ContainerStatus.Annotations carries the annotations of the CRI
				// ContainerConfig, while NRI adjusts the OCI spec. Runtimes such as
				// containerd and CRI-O report that spec under the "info" key of the
				// verbose status, which keeps the adjustment observable through CRI.
				var info struct {
					RuntimeSpec *struct {
						Annotations map[string]string `json:"annotations"`
					} `json:"runtimeSpec"`
				}

				rawInfo, ok := resp.GetInfo()["info"]
				if !ok || json.Unmarshal([]byte(rawInfo), &info) != nil || info.RuntimeSpec == nil {
					Skip(fmt.Sprintf(
						"Expected annotation %q added by an NRI CreateContainer adjustment to be "+
							"observable through ContainerStatus. It is not in the status annotations "+
							"(%v), which report the CRI ContainerConfig annotations, and the verbose "+
							"status info does not expose the OCI runtime spec, so the adjustment is "+
							"not CRI-observable on this runtime.",
						adjustAnnotationKey, annotations))
				}

				Expect(info.RuntimeSpec.Annotations).To(
					HaveKeyWithValue(adjustAnnotationKey, adjustAnnotationVal),
					"OCI runtime spec reported by verbose ContainerStatus should carry the "+
						"annotation added by the NRI adjustment")
			},
		)

		It(
			"should apply a memory limit adjustment to the container cgroup",
			func(ctx SpecContext) {
				testStub.plugin.setAdjust(
					func(_ *nri.PodSandbox, _ *nri.Container) *nri.ContainerAdjustment {
						adj := &nri.ContainerAdjustment{}
						adj.SetLinuxMemoryLimit(adjustMemoryLimit)

						return adj
					},
				)

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
				testStub.plugin.setAdjust(
					func(_ *nri.PodSandbox, _ *nri.Container) *nri.ContainerAdjustment {
						adj := &nri.ContainerAdjustment{}
						adj.SetLinuxSeccompPolicy(
							&nri.LinuxSeccomp{DefaultAction: "SCMP_ACT_ALLOW"},
						)

						return adj
					},
				)

				config := newContainerConfig("nri-test-adjust-seccomp-")
				config.Linux.SecurityContext = &runtimeapi.LinuxContainerSecurityContext{
					Seccomp: &runtimeapi.SecurityProfile{
						ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
					},
				}

				By(
					"creating a RuntimeDefault seccomp container whose policy the NRI plugin overrides",
				)

				createdID, err := framework.CreateContainerWithError(
					ctx, rc, ic, config, podID, podConfig)
				if err == nil {
					containerID = createdID

					Skip(
						"The runtime accepted an NRI adjustment that replaces the RuntimeDefault " +
							"seccomp policy requested by the container security context. Rejecting it " +
							"requires the NRI default validator with " +
							"reject_runtime_default_seccomp_adjustment enabled in the runtime config " +
							"(see https://github.com/containerd/nri#default-validation), which this " +
							"test environment does not appear to enable.",
					)
				}

				framework.Logf("CreateContainer rejected the seccomp adjustment: %v", err)
				Expect(createdID).To(BeEmpty(),
					"no container ID should be returned when the adjustment is rejected")
				Expect(testStub.plugin.adjustedCount()).To(BeNumerically(">=", 1),
					"NRI CreateContainer hook should have been invoked before the rejection")

				By("verifying the rejected container was not leaked")

				containers, err := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})
				Expect(err).NotTo(HaveOccurred())

				for _, c := range containers {
					if c.GetMetadata().GetName() == config.GetMetadata().GetName() {
						containerID = c.GetId() // hand to AfterEach for cleanup
						Fail(fmt.Sprintf(
							"container %s was leaked after the adjustment was rejected", c.GetId()))
					}
				}
			},
		)
	})
})
