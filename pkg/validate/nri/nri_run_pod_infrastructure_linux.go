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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
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

	// infraStateContract names the source of the RunPodSandbox state
	// assertions.
	infraStateContract = "RunPodSandbox Sandbox State Contract, containerd/nri#286 " +
		"(unmerged draft, docs/pod-sandbox-lifecycle.md at 2339aa3)"
)

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var rc internalapi.RuntimeService

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
	})

	// These tests validate the host-visible part of the RunPodSandbox
	// "Sandbox State Contract" of the NRI pod sandbox lifecycle draft
	// (containerd/nri#286): when the RunPodSandbox NRI event fires, the pod
	// namespaces exist, the network is configured with the pod IPs, the pod
	// cgroup exists and the pause container runs. Host state is inspected
	// from the test goroutine while the plugin hook is held open, so specs
	// that read host paths first establish that critest shares the runtime's
	// host view.
	Context("RunPodSandbox infrastructure", Serial, func() {
		var (
			testStub *NRITestStub
			// podIDs lists every sandbox the spec may have created.
			podIDs []string
		)

		addCleanup := func(id string) {
			if id != "" && !slices.Contains(podIDs, id) {
				podIDs = append(podIDs, id)
			}
		}

		AfterEach(func(ctx SpecContext) {
			// Stopping the stub unblocks a hook that may still be holding
			// RunPodSandbox open.
			if testStub != nil {
				testStub.Cleanup()
			}

			for _, id := range podIDs {
				if err := rc.StopPodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", id, err)
				}

				if err := rc.RemovePodSandbox(ctx, id); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", id, err)
				}
			}

			testStub = nil
			podIDs = nil
		})

		// runPodWithBlockedHook starts an NRI plugin that blocks in its
		// RunPodSandbox hook for the pod under test only (matched by UID;
		// other pods pass through). While the hook is blocked, whileBlocked
		// runs on the test goroutine with a copy of the PodSandbox the hook
		// received; it must only collect observations, not assert. The
		// helper then verifies that the observations were taken while the
		// hook was really blocked, releases it, and returns the sandbox ID
		// and the captured pod.
		runPodWithBlockedHook := func(
			ctx SpecContext,
			pluginName string,
			whileBlocked func(pod *nri.PodSandbox),
		) (string, *nri.PodSandbox) {
			podConfig := infraPodConfig(ctx, rc, pluginName)
			targetUID := podConfig.GetMetadata().GetUid()

			var (
				err        error
				hookMu     sync.Mutex
				hookPod    *nri.PodSandbox
				hookCtxErr func() error
				hookBlock  = make(chan struct{})
				hookFired  = make(chan struct{})
			)

			testStub, err = StartNRITestStub(pluginName, "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			testStub.Plugin.OnRunPodSandbox = func(ctx context.Context, pod *nri.PodSandbox) error {
				if pod.GetUid() != targetUID {
					return nil
				}

				hookMu.Lock()
				if hookPod != nil {
					// Only the first delivery for the target pod is held.
					hookMu.Unlock()

					return nil
				}
				// Clone so the test owns its copy independently of the stub.
				hookPod = infraClonePod(pod)
				hookCtxErr = ctx.Err
				hookMu.Unlock()

				close(hookFired)

				select {
				case <-hookBlock:
				case <-ctx.Done():
				}

				return nil
			}

			var (
				runErr   error
				runPodID string
				runDone  = make(chan struct{})
			)

			go func() {
				defer GinkgoRecover()
				defer close(runDone)

				runPodID, runErr = rc.RunPodSandbox(
					ctx,
					podConfig,
					framework.TestContext.RuntimeHandler,
				)
			}()

			// release unblocks the hook, waits for RunPodSandbox and registers
			// every sandbox ID it may have created for cleanup. It also runs
			// when an assertion or Skip unwinds this function.
			released := false

			release := func() {
				if released {
					return
				}

				released = true

				close(hookBlock)
				<-runDone

				if runErr == nil {
					addCleanup(runPodID)
				}

				hookMu.Lock()
				if hookPod != nil {
					addCleanup(hookPod.GetId())
				}
				hookMu.Unlock()
			}
			defer release()

			By("waiting for the RunPodSandbox hook to be reached")

			select {
			case <-hookFired:
			case <-runDone:
				Fail(
					fmt.Sprintf(
						"RunPodSandbox returned (err: %v) before the NRI RunPodSandbox hook "+
							"for the pod was entered",
						runErr,
					),
				)
			case <-time.After(infraHookTimeout):
				Fail("timed out waiting for RunPodSandbox NRI hook to fire")
			}

			hookMu.Lock()
			captured := hookPod
			capturedCtxErr := hookCtxErr
			hookMu.Unlock()

			By("inspecting host state while the RunPodSandbox hook is blocked")
			whileBlocked(captured)

			// Both checks must happen before the hook is released.
			ctxErr := capturedCtxErr()

			returnedWhileBlocked := false

			select {
			case <-runDone:
				returnedWhileBlocked = true
			default:
			}

			release()

			Expect(ctxErr).NotTo(HaveOccurred(),
				"invalid test run, not a runtime deviation: the RunPodSandbox hook context ended "+
					"before the test released the hook (the runtime's plugin request timeout is "+
					"shorter than the checks took), so the observations above may describe the "+
					"state after the hook")
			Expect(returnedWhileBlocked).To(BeFalse(),
				"RunPodSandbox MUST NOT return while an NRI plugin's RunPodSandbox hook is still "+
					"running (%s)", infraStateContract)
			Expect(runErr).NotTo(HaveOccurred(), "RunPodSandbox should succeed after hook returns")
			Expect(runPodID).NotTo(BeEmpty())
			Expect(runPodID).To(Equal(captured.GetId()),
				"sandbox ID returned by RunPodSandbox should match the ID seen by the NRI hook")

			return runPodID, captured
		}

		It(
			"should pass a live network namespace with the pod IPs configured in it to the RunPodSandbox hook",
			func(ctx SpecContext) {
				infraRequireHostView("mnt")

				var (
					nsPath   string
					nsLive   bool
					nsDetail string
					nsAddrs  []string
					addrErr  error
				)

				id, pod := runPodWithBlockedHook(ctx, "cri-test-nri-infra-netns",
					func(pod *nri.PodSandbox) {
						nsPath = infraNamespacePath(pod, infraNetworkNamespaceType)
						if nsPath == "" {
							return
						}

						nsLive, nsDetail = infraIsNamespaceFile(nsPath)
						if nsLive {
							nsAddrs, addrErr = infraNetnsAddrs(nsPath)
						}
					})

				framework.Logf(
					"RunPodSandbox hook: network namespace %q: %s; addresses %v (err: %v)",
					nsPath,
					nsDetail,
					nsAddrs,
					addrErr,
				)

				if nsPath == "" {
					Skip(fmt.Sprintf(
						"precondition not met: the runtime does not populate a %q namespace path in "+
							"PodSandbox.Linux.Namespaces (observed %v), so the pod network namespace "+
							"cannot be located from the hook",
						infraNetworkNamespaceType,
						pod.GetLinux().GetNamespaces(),
					))
				}

				Expect(nsLive).To(BeTrue(),
					"network namespace %s MUST be a live nsfs handle while the RunPodSandbox hook runs "+
						"(%s); observed: %s", nsPath, infraStateContract, nsDetail)

				if errors.Is(addrErr, unix.EPERM) {
					Skip(
						fmt.Sprintf(
							"precondition not met: critest lacks the privilege to enter the "+
								"network namespace %s: %v",
							nsPath,
							addrErr,
						),
					)
				}

				Expect(addrErr).NotTo(HaveOccurred(),
					"failed to list addresses inside network namespace %s", nsPath)

				statusResp, err := rc.PodSandboxStatus(ctx, id, false)
				Expect(err).NotTo(HaveOccurred())

				statusIPs := infraStatusIPs(statusResp.GetStatus())
				if len(statusIPs) == 0 {
					Skip(
						"precondition not met: PodSandboxStatus reports no pod IPs, so pod networking " +
							"(CNI) is not configured and the network inside the namespace cannot be checked",
					)
				}

				Expect(nsAddrs).To(ContainElements(statusIPs),
					"pod IPs %v MUST already be assigned inside network namespace %s while the "+
						"RunPodSandbox hook runs (%s)", statusIPs, nsPath, infraStateContract)
			},
		)

		It(
			"should pass non-empty pod IPs to the RunPodSandbox hook that match PodSandboxStatus",
			func(ctx SpecContext) {
				var hookIPs []string

				id, _ := runPodWithBlockedHook(ctx, "cri-test-nri-infra-ips",
					func(pod *nri.PodSandbox) {
						hookIPs = infraNormalizeIPs(pod.GetIps())
					})

				framework.Logf("RunPodSandbox hook: pod IPs %v", hookIPs)

				By("reading pod IPs from PodSandboxStatus after RunPodSandbox returned")

				statusResp, err := rc.PodSandboxStatus(ctx, id, false)
				Expect(err).NotTo(HaveOccurred())

				statusIPs := infraStatusIPs(statusResp.GetStatus())

				if len(statusIPs) == 0 {
					Expect(hookIPs).To(BeEmpty(),
						"the RunPodSandbox hook received pod IPs that PodSandboxStatus does not report")
					Skip(
						"precondition not met: PodSandboxStatus reports no pod IPs, so pod networking " +
							"(CNI) is not configured and the pod IPs passed to the hook cannot be validated",
					)
				}

				Expect(hookIPs).NotTo(BeEmpty(),
					"PodSandbox.Ips MUST be assigned when the RunPodSandbox hook runs (%s); "+
						"PodSandboxStatus later reported %v", infraStateContract, statusIPs)
				Expect(hookIPs).To(ConsistOf(statusIPs),
					"pod IPs passed to the RunPodSandbox hook MUST match PodSandboxStatus network IPs (%s)",
					infraStateContract)
			},
		)

		It(
			"should pass the sandbox cgroups path to the RunPodSandbox hook and have that cgroup exist",
			func(ctx SpecContext) {
				root := infraRequireCgroupView()

				var (
					cgroupsPath string
					dir         string
					exists      bool
					detail      string
					mapErr      error
				)

				runPodWithBlockedHook(ctx, "cri-test-nri-infra-cgroup",
					func(pod *nri.PodSandbox) {
						cgroupsPath = pod.GetLinux().GetCgroupsPath()
						if cgroupsPath == "" {
							return
						}

						rel, err := infraCgroupRelPath(cgroupsPath)
						if err != nil {
							mapErr = err

							return
						}

						dir = filepath.Join(root, rel)
						exists, detail = infraIsDir(dir)
					})

				framework.Logf(
					"RunPodSandbox hook: cgroups_path=%q -> %q: %s",
					cgroupsPath,
					dir,
					detail,
				)

				if cgroupsPath == "" {
					Skip(
						"precondition not met: the runtime does not populate PodSandbox.Linux.CgroupsPath, " +
							"so the sandbox cgroup cannot be located from the hook",
					)
				}

				Expect(mapErr).NotTo(HaveOccurred(),
					"cannot map the sandbox cgroups path %q to a cgroup directory", cgroupsPath)
				Expect(exists).To(BeTrue(),
					"sandbox cgroup %s (cgroups_path=%q) MUST exist while the RunPodSandbox hook runs "+
						"(%s); observed: %s", dir, cgroupsPath, infraStateContract, detail)
			},
		)

		It(
			"should pass live pod namespaces and a running pause process to the RunPodSandbox hook",
			func(ctx SpecContext) {
				infraRequireHostView("mnt", "pid")

				type nsObservation struct {
					typ, path, detail string
					live, sameAsPause bool
				}

				var (
					namespaces []nsObservation
					pausePID   uint32
					pauseState string
					pauseErr   error
				)

				runPodWithBlockedHook(ctx, "cri-test-nri-infra-pause",
					func(pod *nri.PodSandbox) {
						pausePID = pod.GetPid()
						if pausePID != 0 {
							pauseState, pauseErr = infraProcState(pausePID)
						}

						for _, ns := range pod.GetLinux().GetNamespaces() {
							if ns.GetPath() == "" {
								continue
							}

							o := nsObservation{typ: ns.GetType(), path: ns.GetPath()}
							o.live, o.detail = infraIsNamespaceFile(o.path)

							if pausePID != 0 {
								o.sameAsPause = infraSameFile(
									o.path,
									fmt.Sprintf(
										"/proc/%d/ns/%s",
										pausePID,
										infraKernelNamespace(o.typ),
									),
								)
							}

							namespaces = append(namespaces, o)
						}
					})

				framework.Logf(
					"RunPodSandbox hook: pause pid %d state %q (err: %v); namespaces %+v",
					pausePID,
					pauseState,
					pauseErr,
					namespaces,
				)

				for _, o := range namespaces {
					Expect(o.live).To(BeTrue(),
						"%s namespace %s MUST be a live nsfs handle while the RunPodSandbox hook runs (%s); "+
							"observed: %s", o.typ, o.path, infraStateContract, o.detail)
				}

				if pausePID == 0 {
					Skip(
						fmt.Sprintf(
							"precondition not met: the runtime does not report a pause process in "+
								"PodSandbox.Pid, so the pause container cannot be observed (namespaces with a path "+
								"checked: %d)",
							len(namespaces),
						),
					)
				}

				Expect(pauseErr).NotTo(HaveOccurred(),
					"pause process %d MUST exist while the RunPodSandbox hook runs (%s)",
					pausePID, infraStateContract)
				Expect(pauseState).NotTo(BeElementOf("Z", "X"),
					"pause process %d MUST be running while the RunPodSandbox hook runs (%s)",
					pausePID, infraStateContract)

				for _, o := range namespaces {
					Expect(o.sameAsPause).To(BeTrue(),
						"%s namespace %s passed to the RunPodSandbox hook should be the namespace of the "+
							"pause process %d", o.typ, o.path, pausePID)
				}
			},
		)

		It("should not reuse a sandbox ID after the sandbox is removed", func(ctx SpecContext) {
			// A plugin keys per-pod state by sandbox ID, so a new sandbox that
			// reused a removed one's ID would alias that pod's state.
			var err error

			testStub, err = StartNRITestStub("cri-test-nri-infra-id", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

			podConfig := infraPodConfig(ctx, rc, "cri-test-nri-infra-id")

			By("running, stopping and removing a sandbox")

			first, err := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			addCleanup(first)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc.StopPodSandbox(ctx, first)).To(Succeed())
			Expect(rc.RemovePodSandbox(ctx, first)).To(Succeed())

			By("running a sandbox with the same metadata again")

			second, err := rc.RunPodSandbox(ctx, podConfig, framework.TestContext.RuntimeHandler)
			addCleanup(second)
			Expect(err).NotTo(HaveOccurred())

			Expect(second).NotTo(Equal(first),
				"a new sandbox MUST NOT reuse the ID of a removed sandbox (%s)", infraStateContract)

			var hookIDs []string

			for _, e := range testStub.Plugin.Events() {
				if e.Type == EventRunPodSandbox {
					hookIDs = append(hookIDs, e.PodSandboxID)
				}
			}

			Expect(hookIDs).To(ContainElements(first, second),
				"the RunPodSandbox hook should have seen both sandbox IDs")
		})
	})
})

// infraRequireHostView skips the spec unless critest shares the given
// namespaces (/proc/<pid>/ns names) with the runtime process. The runtime
// process is found independently of the sandbox under test: it is the process
// holding the CRI runtime endpoint socket.
func infraRequireHostView(nsTypes ...string) int {
	pid, err := infraRuntimePID(framework.TestContext.RuntimeServiceAddr)
	if err != nil {
		Skip(
			fmt.Sprintf("precondition not met: cannot establish that critest shares the runtime's "+
				"host view: %v", err),
		)
	}

	for _, t := range nsTypes {
		self, errSelf := os.Readlink("/proc/self/ns/" + t)
		rt, errRt := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", pid, t))

		if errSelf != nil || errRt != nil {
			Skip(
				fmt.Sprintf("precondition not met: cannot compare the %s namespace of critest and "+
					"the runtime (pid %d): %v, %v", t, pid, errSelf, errRt),
			)
		}

		if self != rt {
			Skip(
				fmt.Sprintf(
					"precondition not met: critest does not share the runtime's host view: "+
						"its %s namespace %s differs from the runtime's (pid %d) %s",
					t,
					self,
					pid,
					rt,
				),
			)
		}
	}

	return pid
}

// infraRequireCgroupView skips the spec unless critest sees the runtime's
// cgroup hierarchy, probed through the runtime process's own cgroup
// directory, and returns the hierarchy root.
func infraRequireCgroupView() string {
	pid := infraRequireHostView("mnt", "cgroup")

	root, controller, err := infraCgroupHierarchy()
	if err != nil {
		Skip(
			fmt.Sprintf(
				"precondition not met: no cgroup hierarchy under %s: %v",
				infraCgroupRoot,
				err,
			),
		)
	}

	own, err := infraProcCgroup(pid, controller)
	if err != nil {
		Skip(fmt.Sprintf("precondition not met: cannot read the runtime's own cgroup: %v", err))
	}

	dir := filepath.Join(root, own)
	if ok, detail := infraIsDir(dir); !ok {
		Skip(
			fmt.Sprintf(
				"precondition not met: critest does not see the runtime's cgroup hierarchy: "+
					"the runtime's own cgroup %s (pid %d) is not visible: %s",
				dir,
				pid,
				detail,
			),
		)
	}

	framework.Logf("host view probe: runtime pid %d, own cgroup %s visible", pid, dir)

	return root
}

// infraRuntimePID returns the PID of a process holding the CRI runtime
// endpoint unix socket.
func infraRuntimePID(endpoint string) (int, error) {
	sock := strings.TrimPrefix(endpoint, "unix://")
	if !filepath.IsAbs(sock) {
		return 0, fmt.Errorf("runtime endpoint %q is not a unix socket path", endpoint)
	}

	paths := []string{sock}
	if resolved, err := filepath.EvalSymlinks(sock); err == nil && resolved != sock {
		paths = append(paths, resolved)
	}

	data, err := os.ReadFile("/proc/net/unix")
	if err != nil {
		return 0, err
	}

	links := map[string]bool{}

	for line := range strings.SplitSeq(string(data), "\n") {
		// Num RefCount Protocol Flags Type St Inode Path
		fields := strings.Fields(line)
		if len(fields) >= 8 && slices.Contains(paths, fields[7]) {
			links["socket:["+fields[6]+"]"] = true
		}
	}

	if len(links) == 0 {
		return 0, fmt.Errorf("socket %s is not listed in critest's /proc/net/unix", sock)
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}

	self := os.Getpid()

	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}

		fdDir := fmt.Sprintf("/proc/%d/fd", pid)

		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}

		for _, fd := range fds {
			if link, err := os.Readlink(
				filepath.Join(fdDir, fd.Name()),
			); err == nil &&
				links[link] {
				return pid, nil
			}
		}
	}

	return 0, fmt.Errorf("no process visible to critest holds the runtime socket %s", sock)
}

// infraProcCgroup returns the cgroup of pid in the given v1 controller
// hierarchy, or in the unified hierarchy when controller is empty.
func infraProcCgroup(pid int, controller string) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}

	for line := range strings.SplitSeq(string(data), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}

		if controller == "" {
			if parts[0] == "0" && parts[1] == "" {
				return parts[2], nil
			}

			continue
		}

		for c := range strings.SplitSeq(parts[1], ",") {
			if c == controller || c == "name="+controller {
				return parts[2], nil
			}
		}
	}

	return "", fmt.Errorf("pid %d has no cgroup for controller %q", pid, controller)
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
		Uid:  pod.GetUid(),
		Pid:  pod.GetPid(),
		Ips:  slices.Clone(pod.GetIps()),
	}

	if l := pod.GetLinux(); l != nil {
		clone.Linux = &nri.LinuxPodSandbox{
			CgroupsPath: l.GetCgroupsPath(),
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
	ips := make([]string, 0, 1+len(status.GetNetwork().GetAdditionalIps()))
	ips = append(ips, status.GetNetwork().GetIp())

	for _, extra := range status.GetNetwork().GetAdditionalIps() {
		ips = append(ips, extra.GetIp())
	}

	return infraNormalizeIPs(ips)
}

// infraNormalizeIPs drops empty entries and puts parseable IPs in canonical
// form, so that differently formatted IPv6 addresses compare equal.
func infraNormalizeIPs(in []string) []string {
	var out []string

	for _, s := range in {
		if s == "" {
			continue
		}

		if ip := net.ParseIP(s); ip != nil {
			s = ip.String()
		}

		out = append(out, s)
	}

	return out
}

// infraNetnsAddrs lists the interface addresses inside the network namespace
// at nsPath. It runs on a dedicated, locked OS thread; if that thread cannot
// be returned to its original namespace it is left locked, so the Go runtime
// discards it.
func infraNetnsAddrs(nsPath string) ([]string, error) {
	type result struct {
		addrs []string
		err   error
	}

	ch := make(chan result, 1)

	go func() {
		runtime.LockOSThread()

		addrs, err, restored := infraNetnsAddrsLocked(nsPath)
		if restored {
			runtime.UnlockOSThread()
		}

		ch <- result{addrs, err}
	}()

	r := <-ch

	return r.addrs, r.err
}

func infraNetnsAddrsLocked(nsPath string) (addrs []string, err error, restored bool) {
	orig, err := unix.Open("/proc/thread-self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open own network namespace: %w", err), true
	}
	defer unix.Close(orig)

	target, err := unix.Open(nsPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", nsPath, err), true
	}
	defer unix.Close(target)

	if err := unix.Setns(target, unix.CLONE_NEWNET); err != nil {
		return nil, fmt.Errorf("enter %s: %w", nsPath, err), true
	}

	ifAddrs, listErr := net.InterfaceAddrs()

	if err := unix.Setns(orig, unix.CLONE_NEWNET); err != nil {
		return nil, fmt.Errorf("return from %s: %w", nsPath, err), false
	}

	if listErr != nil {
		return nil, listErr, true
	}

	for _, a := range ifAddrs {
		if ipNet, ok := a.(*net.IPNet); ok {
			addrs = append(addrs, ipNet.IP.String())
		}
	}

	return addrs, nil, true
}

// infraProcState returns the state letter from /proc/<pid>/stat.
func infraProcState(pid uint32) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}

	// "pid (comm) state ...": comm may contain spaces, so cut after the last ')'.
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}

	fields := strings.Fields(string(data[i+1:]))
	if len(fields) == 0 {
		return "", fmt.Errorf("malformed /proc/%d/stat", pid)
	}

	return fields[0], nil
}

func infraSameFile(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}

	fb, err := os.Stat(b)
	if err != nil {
		return false
	}

	return os.SameFile(fa, fb)
}

// infraKernelNamespace maps an OCI namespace type to its /proc/<pid>/ns name.
func infraKernelNamespace(ociType string) string {
	switch ociType {
	case "network":
		return "net"
	case "mount":
		return "mnt"
	default:
		return ociType
	}
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

// infraCgroupHierarchy returns the directory under which pod cgroups appear
// and its controller: the unified mount on cgroup v2 (empty controller),
// otherwise a v1 controller mount.
func infraCgroupHierarchy() (dir, controller string, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(infraCgroupRoot, &st); err != nil {
		return "", "", err
	}

	if st.Type == unix.CGROUP2_SUPER_MAGIC {
		return infraCgroupRoot, "", nil
	}

	// cgroup v1 (or hybrid): every pod cgroup is created in each controller
	// hierarchy, so pick the first one that is mounted.
	for _, controller := range []string{"pids", "memory", "cpu", "systemd"} {
		dir := filepath.Join(infraCgroupRoot, controller)

		var cst unix.Statfs_t
		if err := unix.Statfs(dir, &cst); err == nil && cst.Type == unix.CGROUP_SUPER_MAGIC {
			return dir, controller, nil
		}
	}

	return "", "", errors.New("no cgroup v2 or v1 hierarchy found")
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
