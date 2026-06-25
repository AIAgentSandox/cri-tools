# failed callbacks on create calls

## Implementation Steps

### Task 1: failed callbacks on create calls

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

### Task 1: RunPodSandbox hook error blocks pod creation and allows retry

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

- [x] Add `Context("RunPodSandbox error handling", Serial, ...)` with
  `BeforeEach`/`AfterEach` mirroring the existing RunPodSandbox contract block.
- [x] Implement the `It("should fail RunPodSandbox and clean up when the NRI hook errors, then allow retry")` spec.
- [x] Ensure `AfterEach` cleans up any sandbox left behind (use
  `LastRunPodSandboxID()` fallback) and the stub via `Cleanup()`.

### Task 2: CreateContainer hook error fails container creation and allows retry

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
