# More Test Cases for NRI Plugin

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
  - The specs require a live NRI-enabled runtime + `-nri-socket` to actually
    execute; CI without that runtime will `Skip`. Validation in this loop is
    therefore build + vet + lint clean, plus the spec compiling into the suite.
- Complete each task fully before moving to the next.
- **CRITICAL: all tests must pass before starting next task** (here: build, vet,
  and lint must pass before starting the next task).
- Where a contract is known to differ between containerd and CRI-O, use the
  `SPEC_DISCREPANCY` + `Skip` pattern instead of a hard failure.

## Implementation Steps

### Task 2: RunPodSandbox hook error blocks pod creation and allows retry

Contract (cri-tools#2046 "failure handling" + "retry"; nri#286 "RunPodSandbox
errors may block pod creation"): When an NRI plugin returns an error from its
RunPodSandbox hook, the runtime MUST fail the `RunPodSandbox` CRI call, MUST
clean up the partially-created sandbox (it must not be left listed/Ready), and
MUST allow an immediate retry to succeed once the plugin stops failing.

How to validate:
- Add a new `Context("RunPodSandbox error handling", Serial, ...)` block.
- Start one stub. Set `OnRunPodSandbox` to return a non-nil error (e.g.
  `errors.New("induced NRI RunPodSandbox failure")`), optionally only for the
  first invocation (guard with a `sync.Once`/counter so the retry can pass).
- Call `rc.RunPodSandbox(ctx, podConfig, handler)` and assert it returns an
  error and an empty pod ID.
- Assert the sandbox is not left behind: poll `ListPodSandbox` and confirm no
  sandbox from this test (match by metadata name) is present, or if present it
  is not `SANDBOX_READY`. Use the stub's `LastRunPodSandboxID()` to identify
  the attempted sandbox ID for cleanup.
- Flip the hook to return `nil` (or rely on the `sync.Once`) and call
  `RunPodSandbox` again; assert it now succeeds and the sandbox becomes
  `SANDBOX_READY`.
- If the runtime leaves the failed sandbox in a non-Ready listed state instead
  of removing it, record a `SPEC_DISCREPANCY` and `Skip` the
  "not-listed" assertion while keeping the retry assertion.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add `Context("RunPodSandbox error handling", Serial, ...)` with
  `BeforeEach`/`AfterEach` mirroring the existing RunPodSandbox contract block.
- [ ] Implement the `It("should fail RunPodSandbox and clean up when the NRI hook errors, then allow retry")` spec.
- [ ] Ensure `AfterEach` cleans up any sandbox left behind (use
  `LastRunPodSandboxID()` fallback) and the stub via `Cleanup()`.
- [ ] Build, vet, and lint the package clean.

### Task 3: StopPodSandbox hook error MUST NOT prevent sandbox stop

Contract (nri#286 teardown: "StopPodSandbox errors MUST NOT prevent the sandbox
from being stopped"): When an NRI plugin returns an error from its
StopPodSandbox hook, the `StopPodSandbox` CRI call MUST still succeed and the
sandbox MUST end up stopped (not `SANDBOX_READY`).

How to validate:
- Add an `It` to a new or existing teardown-error `Context`.
- Start one stub, create a pod sandbox (and optionally a running container).
- Set `OnStopPodSandbox` to return an error (guard with `sync.Once` so the
  AfterEach cleanup stop is not affected).
- Call `rc.StopPodSandbox(ctx, podID)` and assert it returns no error despite
  the hook failure.
- Assert `PodSandboxStatus` reports `SANDBOX_NOTREADY` (stopped).
- Assert `RemovePodSandbox` then succeeds.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add a `Context("teardown hook error handling", Serial, ...)` block (this
  context will also host Tasks 4 and 6) with shared `BeforeEach`/`AfterEach`.
- [ ] Implement `It("should stop the sandbox even when the StopPodSandbox NRI hook returns an error")`.
- [ ] Build, vet, and lint the package clean.

### Task 4: RemovePodSandbox hook error MUST NOT prevent sandbox removal

Contract (nri#286 teardown: "RemovePodSandbox proceeds regardless of plugin
failures"): When an NRI plugin returns an error from its RemovePodSandbox hook,
the `RemovePodSandbox` CRI call MUST still succeed and the sandbox MUST be gone
afterward.

How to validate:
- In the `teardown hook error handling` context, add an `It`.
- Start a stub, create + stop a pod sandbox.
- Set `OnRemovePodSandbox` to return an error.
- Call `rc.RemovePodSandbox(ctx, podID)` and assert no error.
- Assert the sandbox is gone: `PodSandboxStatus` returns NotFound, or
  `ListPodSandbox` no longer includes `podID`.
- Clear `podID` so `AfterEach` does not try to remove it again.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Implement `It("should remove the sandbox even when the RemovePodSandbox NRI hook returns an error")`.
- [ ] Build, vet, and lint the package clean.

### Task 5: CreateContainer hook error fails container creation and allows retry

Contract (cri-tools#2046 "NRI errors propagate to creation calls" + "retry must
be allowed immediately after create has failed with an NRI error"): When an NRI
plugin returns an error from its CreateContainer hook, the `CreateContainer`
CRI call MUST fail, MUST NOT leak a half-created container, and a retry (after
the plugin stops failing) MUST succeed.

How to validate:
- Add a new `Context("CreateContainer error handling", Serial, ...)` block.
- Start a stub, create a pod sandbox, pull the test image.
- Set `OnCreateContainer` to return an error (guard with `sync.Once`/counter so
  the retry succeeds).
- Call `rc.CreateContainer(ctx, podID, containerConfig, podConfig)` and assert
  it returns an error and an empty container ID.
- Assert no leaked container: `ListContainers` filtered by `podID` does not
  contain a container with this metadata name (or contains none).
- Flip the hook off and retry `CreateContainer`; assert it now succeeds and the
  container can be started/stopped/removed.
- Optionally assert that the failed attempt produced no surviving started
  container (no `StartContainer` NRI event for a non-existent container).

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add `Context("CreateContainer error handling", Serial, ...)` with
  `BeforeEach` (start stub, pull image, create pod) and `AfterEach` cleanup.
- [ ] Implement `It("should fail CreateContainer when the NRI hook errors, leak nothing, and allow retry")`.
- [ ] Build, vet, and lint the package clean.

### Task 6: StopContainer / RemoveContainer hook errors MUST NOT prevent teardown

Contract (nri#286 teardown: "Plugin errors cannot block pod or container
teardown operations"): When an NRI plugin returns an error from its
StopContainer or RemoveContainer hook, the corresponding CRI call MUST still
succeed and the container MUST reach the expected state (EXITED after stop, gone
after remove).

How to validate:
- In the `teardown hook error handling` context (Task 3), add two `It`s (or one
  combined spec covering both stop and remove).
- Start a stub, create + start a container in a sandbox.
- Set `OnStopContainer` to return an error; call `rc.StopContainer(ctx, id, 0)`
  and assert no error; assert the container is `CONTAINER_EXITED`.
- Set `OnRemoveContainer` to return an error; call `rc.RemoveContainer(ctx, id)`
  and assert no error; assert the container is gone from `ListContainers`.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Implement `It("should stop the container even when the StopContainer NRI hook returns an error")`.
- [ ] Implement `It("should remove the container even when the RemoveContainer NRI hook returns an error")`.
- [ ] Build, vet, and lint the package clean.

### Task 7: No NRI invocation for an invalid CreateContainer target

Contract (cri-tools#2046 "invalid argument handling": NRI must not be invoked
for invalid arguments — container creation for a non-existent sandbox must skip
the NRI CreateContainer callback): A `CreateContainer` call referencing a
non-existent / invalid sandbox ID MUST fail before any NRI CreateContainer hook
fires, so no `CreateContainer` NRI event is delivered.

How to validate:
- Add a new `Context("invalid argument handling", Serial, ...)` block.
- Start a stub.
- Call `rc.CreateContainer(ctx, "nonexistent-sandbox-id", containerConfig,
  podConfig)` and assert it returns an error and empty container ID.
- Use `Consistently(...)` over the stub's `Events()` to assert that zero
  `EventCreateContainer` events were recorded (NRI was never invoked for the
  invalid argument), mirroring the existing "failed CreateContainer did NOT
  generate an NRI event" pattern in the StopPodSandbox idempotency test.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add `Context("invalid argument handling", Serial, ...)` with stub
  start/cleanup.
- [ ] Implement `It("should not invoke the NRI CreateContainer hook for a non-existent sandbox")`.
- [ ] Build, vet, and lint the package clean.

### Task 8: Late-joining plugin is synchronized with existing pods and containers

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

- [ ] Add captured `syncPods`/`syncContainers` fields + accessors to
  `NRITestPlugin`, populated in `Synchronize` under the mutex.
- [ ] Implement `It("should synchronize a newly connected plugin with existing pods and containers")`.
- [ ] Build, vet, and lint the package clean.

### Task 9: Multi-plugin fault isolation on the teardown path

Contract (cri-tools#2046 "notification when a second plugin blocks creation" /
nri#286 "one plugin's failure must not block event delivery to subsequent
plugins, particularly on teardown"): When two plugins are connected and one
returns an error from a teardown hook (StopPodSandbox), the other plugin MUST
still receive the same lifecycle event, and the CRI teardown call MUST still
succeed.

How to validate:
- Add a new `Context("multi-plugin fault isolation", Serial, ...)` block.
- Start two stubs with distinct `pluginIdx` values (e.g. "00" and "10").
- Configure the first plugin's `OnStopPodSandbox` to return an error; leave the
  second plugin's hook nil (just records events).
- Create a pod sandbox, then call `rc.StopPodSandbox(ctx, podID)`; assert it
  succeeds despite plugin 1's error.
- Assert that plugin 2 (the healthy one) still received a `StopPodSandbox`
  event for `podID` (poll `FilterEventsByPodID(stub2.Plugin.Events(), podID)`).
- Assert that plugin 1 also recorded its `StopPodSandbox` event (the event is
  delivered before the hook returns its error).
- Clean up both stubs and the sandbox.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add `Context("multi-plugin fault isolation", Serial, ...)` that starts two
  stubs and cleans both up in `AfterEach`.
- [ ] Implement `It("should deliver teardown events to all plugins even when one plugin fails")`.
- [ ] Build, vet, and lint the package clean.

### Task 10: CreateContainer adjustments returned by a plugin are applied

Contract (NRI core mutation contract, nri#286 / NRI api `ContainerAdjustment`):
A plugin MAY return a `ContainerAdjustment` from its CreateContainer hook (e.g.
to inject environment variables or annotations), and the runtime MUST apply
those adjustments to the created container.

How to validate:
- Extend `NRITestPlugin` so a test can supply a `ContainerAdjustment` that the
  `CreateContainer` implementation returns (add an
  `OnCreateContainerAdjust func(...) *nri.ContainerAdjustment` field, or a
  settable `CreateAdjustment` field; return it from `CreateContainer`).
- Add a new `Context("container adjustments", Serial, ...)` block.
- Configure the plugin to inject an environment variable (e.g.
  `NRI_INJECTED=1`) via the adjustment.
- Create a container whose command echoes the env var to its log (e.g.
  `["sh", "-c", "echo $NRI_INJECTED"]`), start it, wait for exit, and read the
  container log to confirm the injected value is present — proving the
  adjustment was applied.
- If the runtime does not support / apply the adjustment (or env injection is
  not observable on a given runtime), record a `SPEC_DISCREPANCY` and `Skip`.

**Files:**
- Modify: `pkg/validate/nri_util_linux.go` (allow returning a `ContainerAdjustment`)
- Modify: `pkg/validate/nri_linux.go` (new Context/It)

- [ ] Add adjustment-injection capability to `NRITestPlugin.CreateContainer`.
- [ ] Implement `It("should apply environment variable adjustments returned by the NRI CreateContainer hook")` using container log inspection to verify.
- [ ] Build, vet, and lint the package clean.
