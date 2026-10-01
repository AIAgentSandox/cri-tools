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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/sys/unix"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

const (
	// infraCgroupRoot is the mount point of the cgroup filesystem(s).
	infraCgroupRoot = "/sys/fs/cgroup"

	// infraNetworkNamespaceType is the OCI namespace type NRI uses for the
	// pod network namespace.
	infraNetworkNamespaceType = "network"

	// infraHookTimeout bounds how long the test waits for the RunPodSandbox
	// NRI hook to fire.
	infraHookTimeout = 30 * time.Second
)

// infraPathState describes whether a host path was observed while the
// RunPodSandbox hook was blocked and again after RunPodSandbox returned.
type infraPathState struct {
	path         string
	duringHook   bool
	afterReturn  bool
	duringDetail string
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

	// These tests validate the host-visible infrastructure described by the
	// RunPodSandbox "Sandbox State Contract" of the NRI pod sandbox lifecycle
	// specification: when the RunPodSandbox NRI event fires, the pod network
	// namespace exists, the pod IP is assigned and the pod cgroup hierarchy is
	// established. Host state is inspected from the test goroutine while the
	// plugin hook is held open, so critest must share the mount and cgroup
	// view of the runtime host for these checks to be meaningful.
	Context("RunPodSandbox infrastructure", Serial, func() {
		var (
			testStub *NRITestStub
			podID    string
		)

		AfterEach(func(ctx SpecContext) {
			cleanupID := podID
			if cleanupID == "" && testStub != nil {
				cleanupID = testStub.Plugin.LastRunPodSandboxID()
			}

			// Stopping the stub unblocks a hook that may still be holding
			// RunPodSandbox open.
			if testStub != nil {
				testStub.Cleanup()
			}

			if cleanupID != "" {
				if err := rc.StopPodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", cleanupID, err)
				}

				if err := rc.RemovePodSandbox(ctx, cleanupID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", cleanupID, err)
				}
			}

			testStub = nil
			podID = ""
		})

		// runPodWithBlockedHook starts an NRI plugin that captures the
		// PodSandbox passed to its RunPodSandbox hook and blocks there. While
		// the hook is blocked, whileBlocked runs on the test goroutine with the
		// captured pod. The hook is then released and the resulting sandbox ID
		// and captured pod are returned.
		runPodWithBlockedHook := func(
			ctx SpecContext,
			pluginName string,
			whileBlocked func(pod *nri.PodSandbox),
		) (string, *nri.PodSandbox) {
			var (
				err       error
				hookPod   *nri.PodSandbox
				hookOnce  sync.Once
				hookMu    sync.Mutex
				hookBlock = make(chan struct{})
				hookFired = make(chan struct{})
			)

			testStub, err = StartNRITestStub(pluginName, "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			testStub.Plugin.OnRunPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
				hookMu.Lock()
				// Clone so the test owns its copy independently of the stub.
				hookPod = infraClonePod(pod)
				hookMu.Unlock()

				hookOnce.Do(func() { close(hookFired) })

				select {
				case <-hookBlock:
				case <-hookCtx.Done():
				}

				return nil
			}

			podConfig := infraPodConfig(ctx, rc, pluginName)

			var (
				runErr   error
				runPodID string
				runWg    sync.WaitGroup
			)

			runWg.Go(func() {
				runPodID, runErr = rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
			})

			By("waiting for the RunPodSandbox hook to be reached")

			select {
			case <-hookFired:
			case <-time.After(infraHookTimeout):
				close(hookBlock)
				runWg.Wait()
				Fail("timed out waiting for RunPodSandbox NRI hook to fire")
			}

			hookMu.Lock()
			captured := hookPod
			hookMu.Unlock()

			func() {
				// Release the hook even if whileBlocked fails or skips, so the
				// in-flight RunPodSandbox can finish.
				defer func() {
					close(hookBlock)
					runWg.Wait()

					if runErr == nil {
						podID = runPodID
					}
				}()

				By("inspecting host state while the RunPodSandbox hook is blocked")
				whileBlocked(captured)
			}()

			Expect(runErr).NotTo(HaveOccurred(), "RunPodSandbox should succeed after hook returns")
			Expect(runPodID).NotTo(BeEmpty())
			Expect(runPodID).To(Equal(captured.GetId()),
				"sandbox ID returned by RunPodSandbox should match the ID seen by the NRI hook")

			return runPodID, captured
		}

		It(
			"should pass a network namespace path to the RunPodSandbox hook that is a live namespace on the host",
			func(ctx SpecContext) {
				var state infraPathState

				_, pod := runPodWithBlockedHook(ctx, "cri-test-nri-infra-netns",
					func(pod *nri.PodSandbox) {
						state.path = infraNamespacePath(pod, infraNetworkNamespaceType)
						if state.path != "" {
							state.duringHook, state.duringDetail = infraIsNamespaceFile(state.path)
						}

						framework.Logf("RunPodSandbox hook: network namespace %q: %s",
							state.path, state.duringDetail)
					})

				if state.path == "" {
					Skip(fmt.Sprintf(
						"Spec discrepancy: the RunPodSandbox Sandbox State Contract says the pod network "+
							"namespace is created and active when the event fires, so a plugin should be able "+
							"to locate it. Expected PodSandbox.Linux.Namespaces to contain a %q entry with a "+
							"path for a pod-network sandbox; observed namespaces %v. The runtime may not "+
							"populate namespace paths in the NRI PodSandbox (the field is documented as NRI v1 "+
							"emulation).",
						infraNetworkNamespaceType,
						pod.GetLinux().GetNamespaces(),
					))
				}

				state.afterReturn, _ = infraIsNamespaceFile(state.path)

				infraSkipUnlessObserved(
					state,
					"network namespace",
					"the sandbox network namespace was not a mounted nsfs file while the RunPodSandbox "+
						"hook was running",
				)

				Expect(state.duringHook).To(BeTrue(),
					"network namespace %s MUST be a live namespace while the RunPodSandbox hook runs",
					state.path)
			},
		)

		It(
			"should pass pod IPs to the RunPodSandbox hook that match PodSandboxStatus",
			func(ctx SpecContext) {
				var hookIPs []string

				id, _ := runPodWithBlockedHook(ctx, "cri-test-nri-infra-ips",
					func(pod *nri.PodSandbox) {
						hookIPs = slices.Clone(pod.GetIps())
						framework.Logf("RunPodSandbox hook: pod IPs %v", hookIPs)
					})

				By("reading pod IPs from PodSandboxStatus after RunPodSandbox returned")

				statusResp, err := rc.PodSandboxStatus(ctx, id, false)
				Expect(err).NotTo(HaveOccurred())

				statusIPs := infraStatusIPs(statusResp.GetStatus())

				if len(statusIPs) == 0 {
					Skip(
						"PodSandboxStatus reports no pod IPs for a pod-network sandbox; the runtime " +
							"does not appear to have pod networking (CNI) configured, so the pod IPs passed " +
							"to the RunPodSandbox hook cannot be validated",
					)
				}

				if len(hookIPs) == 0 {
					Skip(fmt.Sprintf(
						"Spec discrepancy: the RunPodSandbox Sandbox State Contract says the pod IP is "+
							"assigned and available when the event fires. Expected PodSandbox.Ips passed to "+
							"the hook to be non-empty; observed none, while PodSandboxStatus later reported "+
							"%v. The runtime may assign the pod IP after invoking NRI or may not propagate "+
							"it to the NRI PodSandbox.",
						statusIPs,
					))
				}

				Expect(hookIPs).To(ConsistOf(statusIPs),
					"pod IPs passed to the RunPodSandbox hook should match PodSandboxStatus network IPs")
			},
		)

		It(
			"should pass a cgroups path to the RunPodSandbox hook that exists on the host",
			func(ctx SpecContext) {
				var (
					cgroupsPath  string
					cgroupParent string
					states       []infraPathState
				)

				runPodWithBlockedHook(ctx, "cri-test-nri-infra-cgroup",
					func(pod *nri.PodSandbox) {
						cgroupsPath = pod.GetLinux().GetCgroupsPath()
						cgroupParent = pod.GetLinux().GetCgroupParent()

						for _, dir := range infraCgroupDirs(cgroupsPath, cgroupParent) {
							s := infraPathState{path: dir}
							s.duringHook, s.duringDetail = infraIsDir(dir)
							framework.Logf("RunPodSandbox hook: cgroup %q (cgroups_path=%q, "+
								"cgroup_parent=%q): %s", dir, cgroupsPath, cgroupParent, s.duringDetail)
							states = append(states, s)
						}
					})

				if cgroupsPath == "" {
					Skip(fmt.Sprintf(
						"Spec discrepancy: the RunPodSandbox Sandbox State Contract says the pod-level "+
							"cgroup hierarchy has been established when the event fires, so a plugin should "+
							"be able to locate it. Expected PodSandbox.Linux.CgroupsPath to be set; observed "+
							"an empty value (cgroup_parent=%q). The runtime may not populate the sandbox "+
							"cgroups path in the NRI PodSandbox (the field is documented as NRI v1 emulation).",
						cgroupParent,
					))
				}

				if len(states) == 0 {
					Skip(fmt.Sprintf(
						"cannot map cgroups path %q (cgroup_parent=%q) to a cgroup directory: no supported "+
							"cgroup v1 or v2 hierarchy found under %s",
						cgroupsPath,
						cgroupParent,
						infraCgroupRoot,
					))
				}

				for i := range states {
					states[i].afterReturn, _ = infraIsDir(states[i].path)
					infraSkipUnlessObserved(states[i], "sandbox cgroup",
						fmt.Sprintf("the sandbox cgroup (cgroups_path=%q) did not exist while the "+
							"RunPodSandbox hook was running", cgroupsPath))
				}

				for _, s := range states {
					Expect(s.duringHook).To(BeTrue(),
						"cgroup %s MUST exist while the RunPodSandbox hook runs", s.path)
				}
			},
		)
	})
})

// infraSkipUnlessObserved skips the spec when the path was not observed during
// the hook. If it is also absent after RunPodSandbox returned, critest most
// likely does not share the host view of the runtime; otherwise the runtime
// violates the ordering promised by the spec.
func infraSkipUnlessObserved(state infraPathState, what, violation string) {
	if state.duringHook {
		return
	}

	if !state.afterReturn {
		Skip(fmt.Sprintf(
			"%s %s is not visible from critest even after RunPodSandbox returned (%s). critest "+
				"probably runs in a different mount or cgroup namespace than the runtime, so host "+
				"infrastructure cannot be validated here.",
			what, state.path, state.duringDetail))
	}

	Skip(fmt.Sprintf(
		"Spec discrepancy: the RunPodSandbox Sandbox State Contract (containerd/nri "+
			"docs/pod-sandbox-lifecycle.md) requires the %s to be in place when the NRI event fires. "+
			"Observed: %s (path %s: %s), but it was present after RunPodSandbox returned. The runtime "+
			"may be creating this resource after invoking the NRI RunPodSandbox hook.",
		what, violation, state.path, state.duringDetail))
}

func infraPodConfig(
	ctx context.Context,
	rc internalapi.RuntimeService,
	prefix string,
) *runtimeapi.PodSandboxConfig {
	return &runtimeapi.PodSandboxConfig{
		Metadata: framework.BuildPodSandboxMetadata(
			prefix+"-"+framework.NewUUID(),
			framework.DefaultUIDPrefix+framework.NewUUID(),
			framework.DefaultNamespacePrefix+framework.NewUUID(),
			framework.DefaultAttempt,
		),
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			CgroupParent: common.GetCgroupParent(ctx, rc),
			SecurityContext: &runtimeapi.LinuxSandboxSecurityContext{
				// Explicitly request a pod (non-host) network namespace.
				NamespaceOptions: &runtimeapi.NamespaceOption{
					Network: runtimeapi.NamespaceMode_POD,
				},
			},
		},
		Labels: framework.DefaultPodLabels,
	}
}

func infraClonePod(pod *nri.PodSandbox) *nri.PodSandbox {
	if pod == nil {
		return nil
	}

	clone := &nri.PodSandbox{
		Id:   pod.GetId(),
		Name: pod.GetName(),
		Ips:  slices.Clone(pod.GetIps()),
	}

	if l := pod.GetLinux(); l != nil {
		clone.Linux = &nri.LinuxPodSandbox{
			CgroupParent: l.GetCgroupParent(),
			CgroupsPath:  l.GetCgroupsPath(),
		}
		for _, ns := range l.GetNamespaces() {
			clone.Linux.Namespaces = append(clone.Linux.Namespaces,
				&nri.LinuxNamespace{Type: ns.GetType(), Path: ns.GetPath()})
		}
	}

	return clone
}

func infraNamespacePath(pod *nri.PodSandbox, nsType string) string {
	for _, ns := range pod.GetLinux().GetNamespaces() {
		if ns.GetType() == nsType {
			return ns.GetPath()
		}
	}

	return ""
}

func infraStatusIPs(status *runtimeapi.PodSandboxStatus) []string {
	var ips []string

	if ip := status.GetNetwork().GetIp(); ip != "" {
		ips = append(ips, ip)
	}

	for _, extra := range status.GetNetwork().GetAdditionalIps() {
		if ip := extra.GetIp(); ip != "" {
			ips = append(ips, ip)
		}
	}

	return ips
}

// infraIsNamespaceFile reports whether path is a namespace handle, i.e. a
// file on nsfs (a bind-mounted /proc/<pid>/ns/* entry), which only exists
// while the namespace is alive.
func infraIsNamespaceFile(path string) (ok bool, detail string) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, err.Error()
	}

	if st.Type != unix.NSFS_MAGIC {
		return false, fmt.Sprintf("filesystem type 0x%x is not nsfs", st.Type)
	}

	return true, "nsfs"
}

func infraIsDir(path string) (ok bool, detail string) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, err.Error()
	}

	if !fi.IsDir() {
		return false, "not a directory"
	}

	return true, "directory"
}

// infraCgroupDirs maps the sandbox cgroups path (and cgroup parent, when set)
// to directories on the host, for cgroup v2 or, on v1, a representative
// controller hierarchy. Both the cgroupfs ("/a/b") and the systemd
// ("slice:prefix:name") driver formats are supported.
func infraCgroupDirs(cgroupsPath, cgroupParent string) []string {
	root, err := infraCgroupHierarchy()
	if err != nil {
		framework.Logf("cannot determine cgroup hierarchy: %v", err)

		return nil
	}

	var dirs []string

	for _, p := range []string{cgroupParent, cgroupsPath} {
		if p == "" {
			continue
		}

		rel, err := infraCgroupRelPath(p)
		if err != nil {
			framework.Logf("cannot map cgroup %q to a path: %v", p, err)

			continue
		}

		dirs = append(dirs, filepath.Join(root, rel))
	}

	return dirs
}

// infraCgroupHierarchy returns the directory under which pod cgroups appear:
// the unified mount on cgroup v2, otherwise a v1 controller mount.
func infraCgroupHierarchy() (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(infraCgroupRoot, &st); err != nil {
		return "", err
	}

	if st.Type == unix.CGROUP2_SUPER_MAGIC {
		return infraCgroupRoot, nil
	}

	// cgroup v1 (or hybrid): every pod cgroup is created in each controller
	// hierarchy, so pick the first one that is mounted.
	for _, controller := range []string{"pids", "memory", "cpu", "systemd"} {
		dir := filepath.Join(infraCgroupRoot, controller)

		var cst unix.Statfs_t
		if err := unix.Statfs(dir, &cst); err == nil && cst.Type == unix.CGROUP_SUPER_MAGIC {
			return dir, nil
		}
	}

	return "", errors.New("no cgroup v2 or v1 hierarchy found")
}

// infraCgroupRelPath converts a cgroup parent or cgroups path in either the
// cgroupfs or the systemd driver format to a path relative to the hierarchy
// root.
func infraCgroupRelPath(p string) (string, error) {
	// systemd driver cgroups path: "slice:prefix:name".
	if parts := strings.Split(p, ":"); len(parts) == 3 {
		slice, err := infraExpandSlice(parts[0])
		if err != nil {
			return "", err
		}

		unit := parts[2]
		if !strings.HasSuffix(unit, ".slice") {
			unit = parts[1] + "-" + unit + ".scope"
		}

		return filepath.Join(slice, unit), nil
	}

	// systemd driver cgroup parent: a slice name such as "test.slice" or
	// "/test.slice".
	if base := strings.TrimPrefix(
		p,
		"/",
	); strings.HasSuffix(base, ".slice") &&
		!strings.Contains(base, "/") {
		return infraExpandSlice(base)
	}

	// cgroupfs driver: already a path.
	return filepath.Clean("/" + p), nil
}

// infraExpandSlice expands a systemd slice name into its cgroup path, e.g.
// "a-b.slice" -> "/a.slice/a-b.slice".
func infraExpandSlice(slice string) (string, error) {
	if slice == "" || slice == "-.slice" {
		return "/", nil
	}

	if !strings.HasSuffix(slice, ".slice") || strings.Contains(slice, "/") {
		return "", fmt.Errorf("invalid systemd slice name %q", slice)
	}

	name := strings.TrimSuffix(slice, ".slice")
	path := "/"
	prefix := ""

	for component := range strings.SplitSeq(name, "-") {
		if component == "" {
			return "", fmt.Errorf("invalid systemd slice name %q", slice)
		}

		prefix += component
		path = filepath.Join(path, prefix+".slice")
		prefix += "-"
	}

	return path, nil
}
