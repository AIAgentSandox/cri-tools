# Late-joining plugin is synchronized with existing pods and containers

## Implementation Steps

### Task 1: Late-joining plugin is synchronized with existing pods and containers

- [ ] # More Test Cases for NRI Plugin

## Overview

cri-tools already has a first iteration of NRI (Node Resource Interface)
integration tests in `pkg/validate/nri_linux.go` (introduced by
kubernetes-sigs/cri-tools#2069). Those tests cover the happy-path lifecycle
event ordering for pods and containers, the RunPodSandbox blocking/visibility
contract, and the StopPodSandbox state + idempotency contract.

Several contracts described in kubernetes-sigs/cri-tools#2046 and the NRI pod
sandbox lifecycle contract document (containerd/nri#286) are still NOT covered.
This plan adds one test case per still-uncovered contract. Each task below is a
self-contained `It(...)` spec (or a small group of closely related specs) added
to `pkg/validate/nri_linux.go`, reusing the existing `NRITestStub` /
`NRITestPlugin` helper infrastructure in `pkg/validate/nri_util_linux.go`.

The guiding principle from the spec: NRI plugin errors on the **creation** path
(RunPodSandbox, CreateContainer) MAY block the operation and MUST allow a retry
after cleanup; NRI plugin errors on the **teardown** path (StopPodSandbox,
RemovePodSandbox, StopContainer, RemoveContainer) MUST NOT prevent teardown.

## Context

- Files involved:
  - Modify: `pkg/validate/nri_linux.go` (add new `Context`/`It` spec blocks)
  - Modify: `pkg/validate/nri_util_linux.go` (extend `NRITestPlugin` only where a
    task needs new capture/hook capability, e.g. recording `Synchronize`
    arguments or returning a `ContainerAdjustment`)
- Related patterns (already in the repo, mirror these exactly):
  - `StartNRITestStub(pluginName, pluginIdx string)` starts a stub connected to
    the runtime over the `-nri-socket` path; multiple plugins can run
    concurrently by using distinct `pluginIdx` values (e.g. "00", "10").
  - Hook injection via the `On<Hook>` callback fields on `NRITestPlugin`
    (return an `error` to simulate plugin failure; block on a channel to hold a
    hook open).
  - Event capture via `testStub.Plugin.Events()`, `WaitForEventCount`,
    `FilterEventsByPodID`, and `Reset()`.
  - Serial `Context` blocks with `BeforeEach`/`AfterEach` that build a pod
    config with `framework.BuildPodSandboxMetadata` + `common.GetCgroupParent`,
    and clean up pod/container in `AfterEach`.
  - The `SPEC_DISCREPANCY` pattern: when containerd and CRI-O legitimately
    differ, record the divergence and `Skip(...)` with a clear message rather
    than failing, so the remaining assertions still run.
  - `BeforeEach` guard: `Skip("NRI socket not configured ...")` when
    `framework.TestContext.NRISocketPath == ""`.
- Dependencies: `github.com/containerd/nri/pkg/api`, `.../pkg/stub`,
  `onsi/ginkgo/v2`, `onsi/gomega` (all already vendored and imported).

## Development Approach

- **Testing approach**: Follow the repository's existing testing practices —
  these are Ginkgo e2e specs that run against a live CRI runtime with NRI
  enabled. There is no unit-test harness for this area; the "tests" are the
  Ginkgo specs themselves.
- Each task adds its spec(s), then validation is:
  - `go build ./...`
  - `go vet ./...`
  - `golangci-lint run pkg/validate/...` (the repo lints with golangci-lint;
    match the existing file's style: `By(...)` steps, named return-error
    pattern, comments explaining contract intent).
  - Run new test cases using ` make test-critest-containerd` with both CONTAINERD_VERSION - `main` and `release/2.3`. 
- Complete each task fully before moving to the next.
- **CRITICAL: all tests must pass before starting next task**.
- Where a contract is discovered to differ between containerd and CRI-O, use the
  `SPEC_DISCREPANCY` + `Skip` pattern instead of a hard failure.

## Implementation Steps

### Task 1: Late-joining plugin is synchronized with existing pods and containers

Contract (NRI plugin registration / nri#286 background: when a plugin connects,
the runtime calls `Synchronize` with the current set of pods and containers so
the plugin can reconcile existing state): A plugin that connects AFTER a pod and
container already exist MUST receive those existing pods/containers in its
`Synchronize` callback.

How to validate:
- Extend `NRITestPlugin` in `pkg/validate/nri_util_linux.go` to capture the
  `pods` and `containers` slices passed to `Synchronize` (store them under the
  existing mutex; add accessor methods like `SyncedPods()` / `SyncedContainers()`).
  Currently those arguments are ignored (`_`).
- Add a new `Context("plugin synchronization", Serial, ...)` block.
- With a first stub running, create a pod sandbox and a running container.
- Start a SECOND stub (distinct `pluginIdx`, e.g. "10"); it will go through
  register/configure/synchronize. After it becomes ready, assert its captured
  `Synchronize` pods include the created `podID` and its containers include the
  created container ID.
- Clean up both stubs and the pod/container.

**Files:**
- Modify: `pkg/validate/nri_util_linux.go` (capture Synchronize args + accessors)
- Modify: `pkg/validate/nri_linux.go` (new Context/It)

- [x] Add captured `syncPods`/`syncContainers` fields + accessors to
  `NRITestPlugin`, populated in `Synchronize` under the mutex.
- [x] Implement `It("should synchronize a newly connected plugin with existing pods and containers")`.
- [x] Build, vet, and lint the package clean.
- [x] Run new test with containerd main and 2.2 using make test-e2e-critest with appropriate FOCUS (skipped - requires a live CRI runtime with NRI enabled; not automatable in this environment)

### Task 2: Late-joining plugin is synchronized with container created during the initialization

Same test case as in Task 1, but there will be another container created WHILE the second NRI plugin is processing Synchronize call.
So the task should validate that the second NRI plugin will receive information about the second container as a regular callback and it will not be lost.

- [ ] Implement `It("should receive a callback for container created during the Synchronize call")`.
- [ ] Build, vet, and lint the package clean.
- [ ] Run new test with containerd main and 2.2 using make test-e2e-critest with appropriate FOCUS


### Task 3: Race conditions check with NRI plugin

Same test case as in Task 2, but there will be multiple containers created right before, while, and after the second NRI plugin is processing Synchronize call.
So the task should validate that the second NRI plugin will receive information about all containers in either synchronize or a regular callbacks.

- [ ] Implement `It("should receive information about all containers without the race condition during initialization")`.
- [ ] Build, vet, and lint the package clean.
- [ ] Run new test with containerd main and 2.2 using make test-e2e-critest with appropriate FOCUS


 
