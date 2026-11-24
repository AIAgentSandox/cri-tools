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
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

const stopInfraCgroupRoot = "/sys/fs/cgroup"

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

	Context("StopPodSandbox infrastructure", Serial, func() {
		var (
			testStub *NRITestStub
			podID    string
			// releaseHook unblocks the StopPodSandbox hook; safe to call repeatedly.
			releaseHook func()
			// stopWg tracks the in-flight StopPodSandbox call issued by the test.
			stopWg sync.WaitGroup
			// createdCgroupDirs are host cgroup directories created for a
			// test-unique pod cgroup parent, removed after the pod is removed.
			createdCgroupDirs []string
		)

		BeforeEach(func() {
			testStub = nil
			podID = ""
			releaseHook = func() {}
			createdCgroupDirs = nil
		})

		AfterEach(func(ctx SpecContext) {
			// Unblock the hook first so a StopPodSandbox call held by it can
			// complete, then wait (bounded) for that call before cleaning up.
			releaseHook()

			stopDone := make(chan struct{})

			go func() {
				stopWg.Wait()
				close(stopDone)
			}()

			select {
			case <-stopDone:
			case <-time.After(30 * time.Second):
				framework.Logf(
					"AfterEach: in-flight StopPodSandbox(%s) did not return within 30s",
					podID,
				)
			}

			if testStub != nil {
				testStub.Cleanup()
			}

			if podID != "" {
				if err := rc.StopPodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: StopPodSandbox(%s) failed: %v", podID, err)
				}

				// RemovePodSandbox also removes the workload containers.
				if err := rc.RemovePodSandbox(ctx, podID); err != nil {
					framework.Logf("AfterEach: RemovePodSandbox(%s) failed: %v", podID, err)
				}
			}

			for _, dir := range createdCgroupDirs {
				if err := unix.Rmdir(dir); err != nil && !errors.Is(err, unix.ENOENT) {
					framework.Logf("AfterEach: removing cgroup directory %s failed: %v", dir, err)
				}
			}
		})

		It(
			"should keep the pod network namespace, pod IPs and sandbox cgroup, with workload containers stopped, "+
				"while the StopPodSandbox hook is blocked",
			func(ctx SpecContext) {
				// The NRI pod sandbox lifecycle draft (containerd/nri#286 at
				// revision 2339aa3, an unmerged draft), StopPodSandbox "Sandbox
				// State Contract" and its ordering rules: when the StopPodSandbox
				// hook runs, every workload container has already been stopped,
				// and the network namespace, the pod IPs inside it and the pod
				// cgroup are not reclaimed until the hook returns. A plugin that
				// releases per-pod resources in this hook needs them to still be
				// there. The test blocks the hook for its own pod and inspects the
				// host directly while it is blocked.
				hookBlocking := make(chan struct{})
				hookReached := make(chan *nri.PodSandbox, 1)

				var (
					hookOnce    sync.Once
					releaseOnce sync.Once
					hookMu      sync.Mutex
					runPod      *nri.PodSandbox
					blockPodID  string
					hookExpired atomic.Bool
				)

				releaseHook = func() { releaseOnce.Do(func() { close(hookBlocking) }) }

				var err error

				testStub, err = StartNRITestStub("cri-test-nri-stop-infra", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				testStub.Plugin.OnRunPodSandbox = func(_ context.Context, pod *nri.PodSandbox) error {
					hookMu.Lock()
					defer hookMu.Unlock()

					runPod = pod

					return nil
				}

				// Block only the first StopPodSandbox hook for the test's own
				// pod. Hooks for other pods, and later invocations (e.g.
				// AfterEach cleanup), pass straight through.
				testStub.Plugin.OnStopPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
					hookMu.Lock()
					target := blockPodID
					hookMu.Unlock()

					if target == "" || pod.GetId() != target {
						return nil
					}

					firstInvocation := false

					hookOnce.Do(func() { firstInvocation = true })

					if !firstInvocation {
						return nil
					}

					hookReached <- pod

					select {
					case <-hookBlocking:
					case <-hookCtx.Done():
						// The runtime gave up on the hook: anything observed
						// from now on is post-hook state.
						hookExpired.Store(true)
					}

					return nil
				}

				By("creating a pod sandbox with a pod-level cgroup parent")

				cgroupParent, systemdDriver := stopInfraCgroupParent(ctx, rc)
				podConfig := &runtimeapi.PodSandboxConfig{
					Metadata: framework.BuildPodSandboxMetadata(
						"nri-test-stop-infra-"+framework.NewUUID(),
						framework.DefaultUIDPrefix+framework.NewUUID(),
						framework.DefaultNamespacePrefix+framework.NewUUID(),
						framework.DefaultAttempt,
					),
					Linux: &runtimeapi.LinuxPodSandboxConfig{
						CgroupParent: cgroupParent,
					},
					Labels: framework.DefaultPodLabels,
				}

				if !systemdDriver {
					// The cgroupfs parent is unique to this test; the runtime
					// creates it on demand (in every cgroup v1 hierarchy) and
					// nothing else removes it.
					createdCgroupDirs = stopInfraCgroupHostDirs(cgroupParent, true)
				}

				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				By("creating and starting a workload container in the sandbox")
				framework.PullPublicImage(
					ctx,
					ic,
					framework.TestContext.TestImageList.DefaultTestContainerImage,
					nil,
				)

				containerID := framework.CreateContainer(ctx, rc, ic,
					stopInfraContainerConfig("nri-test-stop-infra-ctr-"+framework.NewUUID()),
					podID, podConfig)
				Expect(containerID).NotTo(BeEmpty())
				Expect(rc.StartContainer(ctx, containerID)).To(Succeed())

				By(
					"recording the pod network namespace, pod IPs and sandbox cgroup before StopPodSandbox",
				)

				hookMu.Lock()
				startedPod := runPod
				hookMu.Unlock()
				Expect(startedPod).NotTo(BeNil(), "RunPodSandbox NRI hook did not fire")
				Expect(startedPod.GetId()).To(Equal(podID))

				netnsPath := stopInfraNetnsPath(startedPod)
				Expect(netnsPath).NotTo(BeEmpty(),
					"RunPodSandbox NRI pod must report a network namespace path")
				Expect(stopInfraCheckNetns(netnsPath)).To(Succeed(),
					"pod network namespace must be valid before StopPodSandbox")

				statusResp, err := rc.PodSandboxStatus(ctx, podID, false)
				Expect(err).NotTo(HaveOccurred(), "PodSandboxStatus before StopPodSandbox")

				podIPs := stopInfraPodIPs(statusResp.GetStatus())
				if len(podIPs) == 0 {
					Skip(fmt.Sprintf("precondition missing: pod %s has no IP in PodSandboxStatus "+
						"(no CNI network configured?), so the pod IP check cannot run", podID))
				}

				Expect(stopInfraCheckIPsInNetns(netnsPath, podIPs)).To(Succeed(),
					"pod IPs must be configured inside the pod network namespace before StopPodSandbox")

				// The sandbox's own cgroup, not CgroupParent: teardown never
				// removes the parent, so checking it would prove nothing.
				sandboxCgroupsPath := startedPod.GetLinux().GetCgroupsPath()
				if sandboxCgroupsPath == "" {
					Skip("precondition missing: the RunPodSandbox NRI pod reports no sandbox " +
						"cgroups path, so the sandbox cgroup cannot be located")
				}

				sandboxCgroupDirs := stopInfraCgroupHostDirs(
					stopInfraCgroupRelPath(sandboxCgroupsPath, systemdDriver), false)
				Expect(stopInfraCheckCgroupDirs(sandboxCgroupDirs)).To(Succeed(),
					"sandbox cgroup %q must exist before StopPodSandbox", sandboxCgroupsPath)

				By("triggering StopPodSandbox in a goroutine")

				hookMu.Lock()
				blockPodID = podID
				hookMu.Unlock()

				var stopErr error

				stopReturned := make(chan struct{})

				stopWg.Go(func() {
					defer close(stopReturned)

					stopErr = rc.StopPodSandbox(ctx, podID)
				})

				By("waiting for the StopPodSandbox hook to be reached")

				select {
				case <-hookReached:
				case <-time.After(30 * time.Second):
					Fail("timed out waiting for the StopPodSandbox NRI hook to fire")
				}

				By("inspecting the sandbox while the hook is blocked")

				// Every check below must finish before the runtime's plugin
				// request timeout ends the hook, so the results are collected
				// first and asserted only after the hook context is known not to
				// have expired.
				netnsErr := stopInfraCheckNetns(netnsPath)

				var ipErr error
				if netnsErr == nil {
					ipErr = stopInfraCheckIPsInNetns(netnsPath, podIPs)
				}

				cgroupErr := stopInfraCheckCgroupDirs(sandboxCgroupDirs)

				containers, listErr := rc.ListContainers(ctx, &runtimeapi.ContainerFilter{
					PodSandboxId: podID,
				})

				hookStatus, hookStatusErr := rc.PodSandboxStatus(ctx, podID, false)
				events := FilterEventsByPodID(testStub.Plugin.Events(), podID)

				stopReturnedWhileBlocked := false

				select {
				case <-stopReturned:
					stopReturnedWhileBlocked = true
				default:
				}

				expired := hookExpired.Load()

				releaseHook()

				Expect(expired).To(BeFalse(), "invalid test run: the StopPodSandbox hook context "+
					"expired before the checks completed, so they may have observed post-hook state; "+
					"raise the runtime's NRI plugin request timeout")
				Expect(stopReturnedWhileBlocked).To(BeFalse(),
					"StopPodSandbox must not return while a plugin's StopPodSandbox hook is in progress")

				Expect(netnsErr).NotTo(HaveOccurred(),
					"pod network namespace must still be a live namespace during the StopPodSandbox hook")
				Expect(ipErr).NotTo(HaveOccurred(),
					"pod IPs must still be configured inside the pod network namespace during the StopPodSandbox hook")
				Expect(cgroupErr).NotTo(HaveOccurred(),
					"sandbox cgroup %q must still exist during the StopPodSandbox hook", sandboxCgroupsPath)

				Expect(
					listErr,
				).NotTo(HaveOccurred(), "ListContainers during the StopPodSandbox hook")

				var workload *runtimeapi.Container

				for _, c := range containers {
					if c.GetId() == containerID {
						workload = c
					}
				}

				Expect(workload).NotTo(BeNil(),
					"workload container %s must still be listed during the StopPodSandbox hook", containerID)
				Expect(workload.GetState()).To(Equal(runtimeapi.ContainerState_CONTAINER_EXITED),
					"workload container %s must be EXITED before the StopPodSandbox hook runs", containerID)

				stopCtrIdx, stopPodIdx := -1, -1

				for i, e := range events {
					switch {
					case e.Type == EventStopContainer && e.ContainerID == containerID && stopCtrIdx < 0:
						stopCtrIdx = i
					case e.Type == EventStopPodSandbox && stopPodIdx < 0:
						stopPodIdx = i
					}
				}

				Expect(stopCtrIdx).To(BeNumerically(">=", 0),
					"StopContainer hook for workload container %s must be delivered before StopPodSandbox "+
						"(events: %+v)", containerID, events)
				Expect(stopCtrIdx).To(BeNumerically("<", stopPodIdx),
					"StopContainer hook for workload container %s must precede the StopPodSandbox hook "+
						"(events: %+v)", containerID, events)

				// CRI API: PodSandboxState is SANDBOX_READY only while the
				// sandbox is running; it is being stopped here.
				Expect(
					hookStatusErr,
				).NotTo(HaveOccurred(), "PodSandboxStatus during the StopPodSandbox hook")
				Expect(
					hookStatus.GetStatus().GetState(),
				).To(Equal(runtimeapi.PodSandboxState_SANDBOX_NOTREADY),
					"sandbox must not be Ready during the StopPodSandbox hook")

				By("waiting for StopPodSandbox to return")
				stopWg.Wait()
				Expect(
					stopErr,
				).NotTo(HaveOccurred(), "StopPodSandbox should succeed after hook returns")

				// Whether the namespace is gone once StopPodSandbox returns is
				// not part of the hook contract, so only record what was observed.
				if _, statErr := os.Stat(netnsPath); statErr == nil {
					framework.Logf(
						"pod network namespace %s still exists after StopPodSandbox returned",
						netnsPath,
					)
				} else {
					framework.Logf(
						"pod network namespace %s after StopPodSandbox returned: %v",
						netnsPath,
						statErr,
					)
				}

				By("verifying CreateContainer in the stopped sandbox fails and delivers no hook")

				// A stopped sandbox is never reused: its infrastructure is being
				// reclaimed, and a container created in it would deliver hooks
				// for a pod that plugins were already told is stopped
				// (containerd/containerd#13599).
				lateName := "nri-test-stop-infra-late-" + framework.NewUUID()
				_, createErr := framework.CreateContainerWithError(ctx, rc, ic,
					stopInfraContainerConfig(lateName), podID, podConfig)
				Expect(createErr).To(HaveOccurred(),
					"CreateContainer in a stopped sandbox must fail")

				// Positive control: RemovePodSandbox must deliver its hook to
				// the same stub, proving it could have seen a CreateContainer.
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after stop")

				removedPodID := podID
				podID = ""

				events = FilterEventsByPodID(testStub.Plugin.Events(), removedPodID)
				Expect(events).To(ContainElement(HaveField("Type", EventRemovePodSandbox)),
					"RemovePodSandbox hook must reach the stub (positive control)")
				Expect(events).NotTo(ContainElement(SatisfyAll(
					HaveField("Type", EventCreateContainer),
					HaveField("ContainerName", lateName),
				)), "a rejected CreateContainer in a stopped sandbox must not deliver a CreateContainer hook")
			},
		)
	})
})

// stopInfraContainerConfig returns a long-running workload container config.
func stopInfraContainerConfig(name string) *runtimeapi.ContainerConfig {
	return &runtimeapi.ContainerConfig{
		Metadata: framework.BuildContainerMetadata(name, framework.DefaultAttempt),
		Image: &runtimeapi.ImageSpec{
			Image:              framework.TestContext.TestImageList.DefaultTestContainerImage,
			UserSpecifiedImage: framework.TestContext.TestImageList.DefaultTestContainerImage,
		},
		Command: framework.DefaultPauseCommand,
		Linux:   &runtimeapi.LinuxContainerConfig{},
	}
}

// stopInfraCgroupParent returns a pod-level cgroup parent for the test pod and
// whether the runtime uses the systemd cgroup driver. With cgroupfs a
// test-unique parent is used so the pod cgroup is not shared with other pods.
func stopInfraCgroupParent(
	ctx context.Context,
	rc internalapi.RuntimeService,
) (parent string, systemd bool) {
	parent = common.GetCgroupParent(ctx, rc)
	if parent != "" {
		return parent, true
	}

	return "/nri-test-stop-infra-" + framework.NewUUID(), false
}

// stopInfraCgroupRelPath maps a runtime cgroups path to a path relative to the
// cgroup root. With systemd it has the form "slice:prefix:name", which is the
// scope "prefix-name.scope" inside the expanded slice.
func stopInfraCgroupRelPath(cgroupsPath string, systemd bool) string {
	if parts := strings.Split(cgroupsPath, ":"); systemd && len(parts) == 3 {
		return filepath.Join(
			stopInfraExpandSystemdSlice(parts[0]),
			parts[1]+"-"+parts[2]+".scope",
		)
	}

	return cgroupsPath
}

// stopInfraCgroupHostDirs maps a cgroup path relative to the cgroup root to the
// directories on the host: one on cgroup v2; on cgroup v1, one per hierarchy
// (all of them when all is set, else only those with the cpu, memory or pids
// controller, where every runtime places the pod).
func stopInfraCgroupHostDirs(rel string, all bool) []string {
	if _, err := os.Stat(filepath.Join(stopInfraCgroupRoot, "cgroup.controllers")); err == nil {
		return []string{filepath.Join(stopInfraCgroupRoot, rel)}
	}

	entries, err := os.ReadDir(stopInfraCgroupRoot)
	if err != nil {
		return nil
	}

	var dirs []string

	for _, entry := range entries {
		// Skip symlinks such as "cpu" -> "cpu,cpuacct": the target is listed too.
		if !entry.IsDir() {
			continue
		}

		hierarchy := filepath.Join(stopInfraCgroupRoot, entry.Name())
		if _, err := os.Stat(filepath.Join(hierarchy, "cgroup.procs")); err != nil {
			continue
		}

		if all || slices.ContainsFunc(strings.Split(entry.Name(), ","), func(c string) bool {
			return c == "cpu" || c == "memory" || c == "pids"
		}) {
			dirs = append(dirs, filepath.Join(hierarchy, rel))
		}
	}

	return dirs
}

// stopInfraExpandSystemdSlice converts a systemd slice name into its cgroup
// path, e.g. "a-b.slice" becomes "a.slice/a-b.slice".
func stopInfraExpandSystemdSlice(slice string) string {
	name := strings.TrimSuffix(slice, ".slice")
	if name == "" || name == "-" {
		return ""
	}

	var (
		parts  []string
		prefix string
	)

	for component := range strings.SplitSeq(name, "-") {
		prefix += component
		parts = append(parts, prefix+".slice")
		prefix += "-"
	}

	return filepath.Join(parts...)
}

// stopInfraCheckCgroupDirs verifies that every directory is a live cgroup.
func stopInfraCheckCgroupDirs(dirs []string) error {
	if len(dirs) == 0 {
		return errors.New("no cgroup hierarchy found under " + stopInfraCgroupRoot)
	}

	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, "cgroup.procs")); err != nil {
			return fmt.Errorf("cgroup %s: %w", dir, err)
		}
	}

	return nil
}

// stopInfraNetnsPath returns the network namespace path reported by NRI.
func stopInfraNetnsPath(pod *nri.PodSandbox) string {
	for _, ns := range pod.GetLinux().GetNamespaces() {
		if ns.GetType() == "network" {
			return ns.GetPath()
		}
	}

	return ""
}

// stopInfraCheckNetns verifies that path is an nsfs network namespace distinct
// from the network namespace of the test process.
func stopInfraCheckNetns(path string) error {
	var pathStat, selfStat unix.Stat_t

	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil {
		return fmt.Errorf("statfs %s: %w", path, err)
	}

	if fs.Type != unix.NSFS_MAGIC {
		return fmt.Errorf("%s is not a namespace file (filesystem type %#x)", path, fs.Type)
	}

	if err := unix.Stat(path, &pathStat); err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	if err := unix.Stat("/proc/self/ns/net", &selfStat); err != nil {
		return fmt.Errorf("stat own network namespace: %w", err)
	}

	if pathStat.Ino == selfStat.Ino && pathStat.Dev == selfStat.Dev {
		return fmt.Errorf("%s is the network namespace of the test process", path)
	}

	return nil
}

// stopInfraPodIPs returns the pod IPs reported by PodSandboxStatus.
func stopInfraPodIPs(status *runtimeapi.PodSandboxStatus) []string {
	var ips []string

	if ip := status.GetNetwork().GetIp(); ip != "" {
		ips = append(ips, ip)
	}

	for _, ip := range status.GetNetwork().GetAdditionalIps() {
		if ip.GetIp() != "" {
			ips = append(ips, ip.GetIp())
		}
	}

	return ips
}

// stopInfraCheckIPsInNetns enters the network namespace at path on a dedicated
// OS thread and verifies that every IP in ips is assigned to an interface.
func stopInfraCheckIPsInNetns(path string, ips []string) error {
	errCh := make(chan error, 1)

	go func() {
		// The thread is never unlocked if restoring the original namespace
		// fails, so the Go runtime discards it instead of reusing it.
		runtime.LockOSThread()

		origNs, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()

			errCh <- fmt.Errorf("open own network namespace: %w", err)

			return
		}
		defer origNs.Close()

		podNs, err := os.Open(path)
		if err != nil {
			runtime.UnlockOSThread()

			errCh <- fmt.Errorf("open %s: %w", path, err)

			return
		}
		defer podNs.Close()

		if err := unix.Setns(int(podNs.Fd()), unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()

			errCh <- fmt.Errorf("enter network namespace %s: %w", path, err)

			return
		}

		addrs, addrErr := net.InterfaceAddrs()

		if err := unix.Setns(int(origNs.Fd()), unix.CLONE_NEWNET); err != nil {
			errCh <- fmt.Errorf("restore own network namespace: %w", err)

			return
		}

		runtime.UnlockOSThread()

		if addrErr != nil {
			errCh <- fmt.Errorf("list addresses in %s: %w", path, addrErr)

			return
		}

		configured := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				configured = append(configured, ipNet.IP.String())
			}
		}

		for _, ip := range ips {
			want := net.ParseIP(ip)
			found := false

			for _, c := range configured {
				if want != nil && want.Equal(net.ParseIP(c)) {
					found = true

					break
				}
			}

			if !found {
				errCh <- fmt.Errorf("IP %s not configured in %s (addresses: %v)", ip, path, configured)

				return
			}
		}

		errCh <- nil
	}()

	return <-errCh
}
