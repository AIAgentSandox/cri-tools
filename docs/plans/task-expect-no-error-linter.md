# Add a linter to prevent double-err passing to framework.ExpectNoError

## Overview

Follow-up on kubernetes-sigs/cri-tools#2121. The test framework helper
`framework.ExpectNoError(err error, explain ...any)` (in `pkg/framework/util.go`)
lets callers accidentally pass the error twice, e.g.:

```go
framework.ExpectNoError(err, "failed to start Container: %v", err)
```

PR #2121 manually removed every such occurrence. This plan prevents the pattern
from coming back by adding a custom Go static analyzer that flags any call to
`framework.ExpectNoError` where the error expression passed as the first
argument also appears among the variadic `explain` arguments.

### Why a linter instead of removing the helper

`ExpectNoError` is used **107 times across 15 files** (all of `pkg/validate/` and
`pkg/benchmark/`, plus `pkg/framework/util.go` itself). It also adds value beyond
raw `gomega.Expect(...)`: it logs `Unexpected error occurred: %v` before
asserting. Given the wide usage and the extra behavior, removing it would be a
large, churny change that loses functionality. The task description says: "if the
helper is widely used a linter may be preferable" — it is, so we add a linter.

## Context

- Files involved:
  - Create: `test/lint/expectnoerror/expectnoerror.go` — the analyzer
  - Create: `test/lint/expectnoerror/expectnoerror_test.go` — `analysistest` unit test
  - Create: `test/lint/expectnoerror/testdata/src/a/a.go` — test fixtures with
    `// want` comments (good and bad calls)
  - Create: `test/lint/expectnoerror/testdata/src/a/framework/framework.go` — a
    minimal stub `ExpectNoError` so testdata compiles without the real framework
    and its heavy CRI dependency tree
  - Create: `test/lint/expectnoerror/cmd/main.go` — `singlechecker` runner so the
    analyzer can run as a standalone binary / `go vet` tool
  - Modify: `Makefile` — add a `verify-expectnoerror` target and add it to the
    aggregate `verify` target
  - Modify: `.github/workflows/build.yml` — add `expectnoerror` to the `verify`
    matrix so CI runs it
  - Modify: `go.mod` / `go.sum` / `vendor/` — promote `golang.org/x/tools` to a
    direct dependency and vendor `go/analysis`, `go/analysis/singlechecker`,
    `go/analysis/analysistest`, and `go/analysis/passes/inspect`
- Related patterns:
  - The repo already uses golangci-lint v2 heavily (`.golangci.yml`) with custom
    `forbidigo` rules, and has `make verify-*` targets wired into the CI matrix in
    `.github/workflows/build.yml` (`make verify-${{ matrix.run }}`).
  - `golang.org/x/tools` is already an indirect dependency (`go.mod`,
    `v0.45.0`); `go/ast/inspector` and `go/packages` are already vendored.
  - New Go files must carry the Apache boilerplate header
    (`hack/boilerplate/boilerplate.go.txt`); `make verify-boilerplate` checks it.
- Dependencies: `golang.org/x/tools/go/analysis` (already transitively present;
  needs vendoring of the analysis subpackages). No new third-party modules.

## Development Approach

- **Testing approach**: standard Go analyzer testing with
  `golang.org/x/tools/go/analysis/analysistest` driven by `go test`. This matches
  the conventions of every analyzer in `golang.org/x/tools`.
- Run `go build ./...`, `go test ./test/lint/...`, and `make verify-lint` (the
  analyzer code itself must pass the repo's golangci-lint config).
- Complete each task fully before moving to the next.
- **CRITICAL: all tests must pass before starting next task.**

## Implementation Steps

### Task 1: Implement the expectnoerror analyzer with unit tests

Write the static analyzer and prove it works in isolation with `analysistest`,
before wiring it into the build. Keep the analysis dependencies out of the main
module until they are actually compiled here — but since the analyzer lives in
the main module, this task also introduces the vendored analysis packages.

**Files:**
- Create: `test/lint/expectnoerror/expectnoerror.go`
- Create: `test/lint/expectnoerror/expectnoerror_test.go`
- Create: `test/lint/expectnoerror/testdata/src/a/a.go`
- Create: `test/lint/expectnoerror/testdata/src/a/framework/framework.go`
- Modify: `go.mod`, `go.sum`, `vendor/` (via `go mod tidy && go mod vendor`)

**Analyzer behavior (`expectnoerror.go`):**
- Export `var Analyzer = &analysis.Analyzer{Name: "expectnoerror", ...}`.
- Depend on `inspect.Analyzer` (`go/analysis/passes/inspect`) and use
  `inspector.Inspector` to walk `*ast.CallExpr` nodes.
- For each call, resolve the callee via `pass.TypesInfo` (handle both
  `framework.ExpectNoError(...)` selector calls and a bare `ExpectNoError(...)`
  call from inside the `framework` package). Confirm the resolved
  `*types.Func` has name `ExpectNoError`. To remain robust without hardcoding a
  brittle full package path, match on function name `ExpectNoError` plus the
  package path **suffix** `pkg/framework` (the package that declares it).
- Let `errArg = call.Args[0]`. For each remaining argument `call.Args[i]`
  (i >= 1, the `explain ...any` values), report a diagnostic if that argument is
  syntactically equivalent to `errArg`. Use object identity for identifiers
  (`pass.TypesInfo.ObjectOf` on `*ast.Ident`) and structural equality for simple
  selector expressions (e.g. `resp.Err`). This keeps false positives near zero:
  `ExpectNoError(err, "msg %v", otherErr)` is **not** flagged.
- Diagnostic message, e.g.:
  `error passed to ExpectNoError is also passed in the explain arguments; drop the duplicate err (and its %v) — ExpectNoError already includes the error`.
- Add the Apache boilerplate header.

**Testdata:**
- `testdata/src/a/framework/framework.go`: minimal package `framework` declaring
  `func ExpectNoError(err error, explain ...any) {}` so the analyzer's
  package-path-suffix match (`pkg/framework`) works; place the file so its import
  path ends in `.../pkg/framework`. (If reproducing the suffix path in testdata is
  awkward, the analyzer match MAY instead accept package name `framework` +
  func name `ExpectNoError`; pick whichever the test can express cleanly and keep
  the production behavior identical for the real `sigs.k8s.io/cri-tools/pkg/framework`.)
- `testdata/src/a/a.go`: calls covering — bad: `ExpectNoError(err, "x: %v", err)`
  with `// want "passed to ExpectNoError"`; good (no diagnostic):
  `ExpectNoError(err, "x")`, `ExpectNoError(err, "x: %v", otherErr)`,
  `ExpectNoError(err)`.
- Add Apache boilerplate headers to testdata `.go` files.

**Checkboxes:**
- [ ] Vendor the analysis packages: add a blank/import use, run
  `go mod tidy && go mod vendor` so `golang.org/x/tools` becomes a direct dep and
  `go/analysis`, `go/analysis/passes/inspect`, and `go/analysis/analysistest`
  land in `vendor/` and `vendor/modules.txt`.
- [ ] Implement `expectnoerror.go` with the detection logic described above.
- [ ] Create the `testdata` fixtures (`a.go` + `framework` stub) with `// want`
  comments for the bad call and none for the good calls.
- [ ] Implement `expectnoerror_test.go` using
  `analysistest.Run(t, analysistest.TestData(), expectnoerror.Analyzer, "a")`.
- [ ] Run `go test ./test/lint/expectnoerror/...` and confirm it passes.

### Task 2: Wire the analyzer into the build, Makefile, and CI

Add a runnable command and a `make` target so the analyzer runs over the whole
repo, then add it to CI. Verify it is green on the current (already-cleaned) tree
and that it would have caught the #2121 pattern.

**Files:**
- Create: `test/lint/expectnoerror/cmd/main.go`
- Modify: `Makefile`
- Modify: `.github/workflows/build.yml`

**`cmd/main.go`:**
- `package main`; call `singlechecker.Main(expectnoerror.Analyzer)` from
  `golang.org/x/tools/go/analysis/singlechecker`. Add the Apache header.
- This yields a binary usable directly (`expectnoerror ./...`) and as
  `go vet -vettool=<bin>`.

**Makefile:**
- Add a `verify-expectnoerror` PHONY target that runs the analyzer over the module
  packages, e.g.:
  ```make
  .PHONY: verify-expectnoerror
  verify-expectnoerror: ## Verify err is not passed twice to framework.ExpectNoError.
  	$(GO) run ./test/lint/expectnoerror/cmd ./...
  ```
  (`go run` avoids shipping a binary; `singlechecker` exits non-zero on findings,
  failing the target. Match the existing target style and `## help` comment
  convention.)
- Add `verify-expectnoerror` to the aggregate `verify` target's dependency list
  (line 149).

**CI (`.github/workflows/build.yml`):**
- Add `expectnoerror` to the `verify` job's `matrix.run` list (alongside `lint`,
  `boilerplate`, etc.) so `make verify-expectnoerror` runs in CI.

**Checkboxes:**
- [ ] Create `cmd/main.go` using `singlechecker.Main`.
- [ ] Add the `verify-expectnoerror` Makefile target and add it to `verify`.
- [ ] Add `expectnoerror` to the CI verify matrix in `build.yml`.
- [ ] Run `make verify-expectnoerror` and confirm it passes clean on the current
      tree (the #2121 fixes are already merged, so there should be zero findings).
- [ ] Sanity-check the detector by temporarily re-introducing one
      `ExpectNoError(err, "...: %v", err)` call, confirming the target fails on it,
      then reverting the temporary change.
- [ ] Run `make verify-lint` to confirm the new Go files pass the repo's
      golangci-lint configuration (boilerplate, wsl, gofumpt/gci import order, etc.).
- [ ] Run `make verify-boilerplate` to confirm headers on all new `.go` files.

## Success criteria

- [ ] `make verify-expectnoerror` passes on the current tree and is part of
      `make verify` and the CI verify matrix.
- [ ] The analyzer flags `framework.ExpectNoError(err, "...: %v", err)` and does
      not flag legitimate calls (`ExpectNoError(err, "...")`,
      `ExpectNoError(err, "...%v", otherErr)`).
- [ ] `go test ./test/lint/expectnoerror/...` passes via `analysistest`.
- [ ] `make verify-lint` and `make verify-boilerplate` remain green.

## Notes / alternatives considered

- **Remove `ExpectNoError` entirely**: rejected — 107 call sites and it provides
  extra logging; large churn for little gain.
- **golangci-lint v2 module plugin (`.custom-gcl.yml`)**: rejected for now — it
  requires building a custom `golangci-lint` binary instead of the pinned
  prebuilt one the Makefile downloads, a heavier change than a standalone
  `singlechecker`. The analyzer is written as a standard `analysis.Analyzer`, so
  it can later be adopted as a golangci-lint plugin with no logic changes if
  desired.
