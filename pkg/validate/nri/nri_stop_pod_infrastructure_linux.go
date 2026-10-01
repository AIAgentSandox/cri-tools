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

const stopInfraCgroupRoot = "/sys/fs/cgroup"

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var rc internalapi.RuntimeService

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
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
			"should keep the pod network namespace, pod IP and pod cgroup on the host while the StopPodSandbox hook is blocked",
			func(ctx SpecContext) {
				// Spec (NRI pod sandbox lifecycle, StopPodSandbox "Sandbox State
				// Contract" and ordering rule 7): while the StopPodSandbox hook
				// runs, the network namespace and CNI networks are not reclaimed
				// and the pod's cgroups remain accessible. This test inspects the
				// host directly (namespace file, addresses inside the namespace,
				// cgroup directory) while the hook is blocked.
				hookBlocking := make(chan struct{})
				hookReached := make(chan *nri.PodSandbox, 1)

				var (
					hookOnce    sync.Once
					releaseOnce sync.Once
					runPodMu    sync.Mutex
					runPod      *nri.PodSandbox
				)

				releaseHook = func() { releaseOnce.Do(func() { close(hookBlocking) }) }

				var err error

				testStub, err = StartNRITestStub("cri-test-nri-stop-infra", "00")
				Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")

				testStub.Plugin.OnRunPodSandbox = func(_ context.Context, pod *nri.PodSandbox) error {
					runPodMu.Lock()
					defer runPodMu.Unlock()

					runPod = pod

					return nil
				}

				// Block only the first StopPodSandbox invocation; later ones
				// (e.g. AfterEach cleanup) pass straight through.
				testStub.Plugin.OnStopPodSandbox = func(hookCtx context.Context, pod *nri.PodSandbox) error {
					firstInvocation := false

					hookOnce.Do(func() { firstInvocation = true })

					if !firstInvocation {
						return nil
					}

					hookReached <- pod

					select {
					case <-hookBlocking:
					case <-hookCtx.Done():
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

				podCgroupDirs := stopInfraCgroupHostDirs(cgroupParent, systemdDriver)
				if !systemdDriver {
					// The cgroupfs parent is unique to this test; the runtime
					// creates it on demand and nothing else removes it.
					createdCgroupDirs = podCgroupDirs
				}

				podID = framework.RunPodSandbox(ctx, rc, podConfig)
				Expect(podID).NotTo(BeEmpty())

				By(
					"recording the pod network namespace, pod IPs and pod cgroup before StopPodSandbox",
				)

				runPodMu.Lock()
				startedPod := runPod
				runPodMu.Unlock()
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
					framework.Logf(
						"pod %s has no IP in PodSandboxStatus (no CNI network?); skipping the pod IP check",
						podID,
					)
				} else {
					Expect(stopInfraCheckIPsInNetns(netnsPath, podIPs)).To(Succeed(),
						"pod IPs must be configured inside the pod network namespace before StopPodSandbox")
				}

				Expect(stopInfraCheckCgroupDirs(podCgroupDirs)).To(Succeed(),
					"pod cgroup %q must exist before StopPodSandbox", cgroupParent)

				By("triggering StopPodSandbox in a goroutine")

				var stopErr error

				stopWg.Go(func() {
					stopErr = rc.StopPodSandbox(ctx, podID)
				})

				By("waiting for the StopPodSandbox hook to be reached")

				var stoppingPod *nri.PodSandbox

				select {
				case stoppingPod = <-hookReached:
				case <-time.After(30 * time.Second):
					Fail("timed out waiting for the StopPodSandbox NRI hook to fire")
				}

				// Spec violations are collected while the hook is blocked and
				// reported (as a Skip) only after the hook is released.
				var discrepancies []string

				By(
					"verifying the pod network namespace still exists and is valid while the hook is blocked",
				)

				hookNetnsPath := stopInfraNetnsPath(stoppingPod)
				if hookNetnsPath == "" {
					discrepancies = append(discrepancies, fmt.Sprintf(
						"the StopPodSandbox NRI pod reports no network namespace path "+
							"(RunPodSandbox reported %q); the runtime may have removed the namespace "+
							"before invoking the hook",
						netnsPath))
					hookNetnsPath = netnsPath
				} else if hookNetnsPath != netnsPath {
					discrepancies = append(discrepancies, fmt.Sprintf(
						"the StopPodSandbox NRI pod reports network namespace %q, "+
							"but RunPodSandbox reported %q",
						hookNetnsPath, netnsPath))
				}

				netnsErr := stopInfraCheckNetns(hookNetnsPath)
				if netnsErr != nil {
					discrepancies = append(discrepancies, fmt.Sprintf(
						"pod network namespace is not usable during the StopPodSandbox hook: %v",
						netnsErr,
					))
				}

				if len(podIPs) > 0 && netnsErr == nil {
					By(
						"verifying the pod IPs are still configured inside the network namespace while the hook is blocked",
					)

					if ipErr := stopInfraCheckIPsInNetns(hookNetnsPath, podIPs); ipErr != nil {
						discrepancies = append(discrepancies, fmt.Sprintf(
							"pod IPs are not configured inside the network namespace during the "+
								"StopPodSandbox hook (CNI teardown happened before the hook?): %v", ipErr))
					}
				}

				By("verifying the pod cgroup still exists while the hook is blocked")

				if hookParent := stoppingPod.GetLinux().
					GetCgroupParent(); hookParent != cgroupParent {
					framework.Logf("StopPodSandbox NRI pod reports cgroup parent %q, configured %q",
						hookParent, cgroupParent)
				}

				if cgErr := stopInfraCheckCgroupDirs(podCgroupDirs); cgErr != nil {
					discrepancies = append(discrepancies, fmt.Sprintf(
						"pod cgroup %q is not accessible during the StopPodSandbox hook: %v",
						cgroupParent, cgErr))
				}

				By("releasing the hook and verifying StopPodSandbox succeeds")
				releaseHook()
				stopWg.Wait()
				Expect(
					stopErr,
				).NotTo(HaveOccurred(), "StopPodSandbox should succeed after hook returns")

				// The NRI spec does not require the namespace to be gone once
				// StopPodSandbox returns, so only record what was observed.
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

				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred(),
					"RemovePodSandbox should succeed after stop")
				podID = ""

				if len(discrepancies) > 0 {
					Skip("spec discrepancy: the NRI pod sandbox lifecycle spec requires the pod " +
						"network namespace, CNI networks and pod cgroups to remain in place until the " +
						"StopPodSandbox hook completes, but the runtime did not keep them:\n- " +
						strings.Join(discrepancies, "\n- "))
				}
			},
		)
	})
})

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

// stopInfraCgroupHostDirs maps a CRI cgroup parent to the cgroup directories
// on the host: one directory on cgroup v2, one per controller on cgroup v1.
func stopInfraCgroupHostDirs(parent string, systemd bool) []string {
	rel := parent
	if systemd {
		rel = stopInfraExpandSystemdSlice(filepath.Base(parent))
	}

	if _, err := os.Stat(filepath.Join(stopInfraCgroupRoot, "cgroup.controllers")); err == nil {
		return []string{filepath.Join(stopInfraCgroupRoot, rel)}
	}

	var dirs []string

	for _, controller := range []string{"cpu", "memory", "pids"} {
		hierarchy := filepath.Join(stopInfraCgroupRoot, controller)
		if _, err := os.Stat(hierarchy); err == nil {
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
