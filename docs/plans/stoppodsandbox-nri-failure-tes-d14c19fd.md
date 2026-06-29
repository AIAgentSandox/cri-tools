# StopPodSandbox NRI failure test

## Implementation Steps

### Task 1: StopPodSandbox NRI failure test

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

### Task 3: StopPodSandbox hook error MUST prevent sandbox stop

The StopPodSandbox hook MUST prevent sandbox stop on NRI failure. However the sandbox must be non-operational, i.e. no new containers can be created any longer.

How to validate:
- Add an `It` to a new or existing teardown-error `Context`.
- Start one stub, create a pod sandbox (and optionally a running container).
- Set `OnStopPodSandbox` to return an error (guard with `sync.Once` so the
  AfterEach cleanup stop is not affected).
- Call `rc.StopPodSandbox(ctx, podID)` ensure it failed.
- Ensure that new containers cannot be created after it.
- Assert the state of sandbox is still running.
- Retry Stopping
- Assert `PodSandboxStatus` reports `SANDBOX_NOTREADY` (stopped).
- Assert `RemovePodSandbox` then succeeds.

**Files:**
- Modify: `pkg/validate/nri_linux.go`

- [ ] Add a `Context("teardown hook error handling", Serial, ...)` block (this
  context will also host Tasks 4 and 6) with shared `BeforeEach`/`AfterEach`.
- [ ] Implement `It("should stop the sandbox even when the StopPodSandbox NRI hook returns an error")`.
- [ ] Build, vet, and lint the package clean.
