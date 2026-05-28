# Linter to Block log.Fatalf in cri-tools

## Overview
Add a `golangci-lint` rule that blocks new usages of `log.Fatalf`, `logrus.Fatal*`, and related `Fatal*` calls in the cri-tools Go codebase. PR #2106 replaced all such calls in `cmd/crictl/` with proper error returns; this plan prevents regressions by failing CI when these patterns are reintroduced. The implementation enables the `forbidigo` linter (already listed but commented out in `.golangci.yml`) with regex patterns scoped to the relevant Fatal-style functions, and adds a self-test to confirm the rule fires.

## Context
- Files involved:
  - Modify: `.golangci.yml` — enable and configure `forbidigo`.
- Related patterns:
  - The repo uses golangci-lint v2 (`v2.12.2`, pinned in `Makefile:57`) with a `linters.default: none` + explicit `enable:` list and per-linter settings under `linters.settings`. The Fatal-block rule follows the same convention by adding `forbidigo` to `enable:` and a `forbidigo:` block under `settings:`.
  - `linters.exclusions.rules` is the existing mechanism used to scope linters by file path (see the `goconst` carve-outs); the same mechanism scopes the new rule to non-test, non-vendor Go code.
  - CI runs `make verify-lint`, which invokes `$(GOLANGCI_LINT) run` for the current OS, Linux, and Windows (`Makefile:151-159`). No additional CI plumbing is required — enabling the linter in the config is sufficient to gate PRs.
- Verified state of the tree:
  - `grep -rn "log\.Fatal\|logrus\.Fatal" --include="*.go" --exclude-dir=vendor` returns zero hits (PR #2106 removed them all). The only remaining `Fatal` occurrences in non-vendor code are `t.Fatal[f]` in `*_test.go`, which are `*testing.T` methods and will not match the new regex patterns.
- Dependencies:
  - `forbidigo` is already bundled with `golangci-lint v2.12.2`; no new tooling is required.

## Development Approach

- **Testing approach**: Follow the repository's existing testing practices
- Complete each task fully before moving to the next
- **CRITICAL: all tests must pass before starting next task**
- Verification is via `make verify-lint`. This repo has no Go-level unit tests for lint config, so validation is done by running the linter against the tree before and after a temporary regression fixture.

## Implementation Steps

### Task 1: Enable and configure `forbidigo` in `.golangci.yml`
Activate the linter that will block `Fatal`-style abort calls, and define patterns for the call sites that PR #2106 cleaned up. Scope the rule so it applies to all non-vendor, non-test Go code (test files use `t.Fatal*` legitimately; the regexes won't match it, but excluding `_test.go` keeps intent explicit and matches the existing per-path rule style).

**Files:**
- Modify: `.golangci.yml`

- [ ] In `linters.enable`, add `- forbidigo` (alphabetically, between `fatcontext` and `forcetypeassert`). Remove the matching `# - forbidigo` line in the disabled block at the bottom of the `enable:` list so the two lists stay consistent with the existing style.
- [ ] Under `linters.settings`, add a `forbidigo:` block that disables type analysis (regex-only is sufficient and keeps the rule cheap) and defines the forbidden patterns. Use named `pattern` / `msg` entries so messages are descriptive:
  - `^logrus\.Fatal.*$` — message: "use error returns instead of logrus.Fatal*; let main() print and exit with non-zero (see PR #2106)"
  - `^log\.Fatal.*$` — message: "use error returns instead of log.Fatal*; let main() print and exit with non-zero (see PR #2106)"
  - `^klog\.Fatal.*$` — message: "use error returns instead of klog.Fatal*; let main() print and exit with non-zero (see PR #2106)"
  - Set `exclude-godoc-examples: true` (default) and `analyze-types: false` so the rule is purely textual and doesn't slow the linter run.
- [ ] Under `linters.exclusions.rules`, add a carve-out exempting `_test\.go$` from `forbidigo` for symmetry with the other path-scoped exclusions already present (defensive — current regexes don't match `t.Fatal*`, but this guards against future patterns like `klog.Fatal` in test helpers).
- [ ] Keep ordering and indentation consistent with the rest of the file (two-space indent, alphabetical within `enable:`, settings sub-blocks alphabetical under `settings:`).
- [ ] Run `make install.lint` if `golangci-lint` is not yet installed locally, then run `make verify-lint` and confirm it still passes on a clean tree (no regressions on the current code, which has zero matching patterns).
- [ ] Tests: this repo has no unit-test harness for lint config, so no Go test changes are required for this task. Validation is performed in Task 2.

### Task 2: Manually verify the rule fires on a regression
Confirm that the configured linter actually rejects `log.Fatalf` / `logrus.Fatalf` / `klog.Fatalf` if they are reintroduced. This is a throwaway local check — do not commit the fixture.

**Files:**
- Temporary (do not commit): a scratch edit to one file under `cmd/crictl/`, e.g. `cmd/crictl/main.go`.

- [ ] In a working-copy-only edit, replace the `logrus.Error(err)` + `os.Exit(1)` pair in `cmd/crictl/main.go:60-63` with `logrus.Fatal(err)` (single line).
- [ ] Run `make verify-lint`. Confirm `forbidigo` reports the new line with the configured message and that exit status is non-zero.
- [ ] Revert the scratch edit with `git checkout -- cmd/crictl/main.go` and re-run `make verify-lint` to confirm a clean tree still passes.
- [ ] Repeat the spot-check for `log.Fatalf("x")` and `klog.Fatal("x")` (using temporary edits) to confirm all three regexes fire, then revert.
- [ ] Tests: none — this task is a manual lint-rule self-test, not a unit test.

### Task 3: Final repo check and commit
Run the full verify suite to confirm no other linter is upset by the config changes, then commit.

**Files:**
- No additional file changes beyond Task 1.

- [ ] Run `make verify-lint` for the current OS plus the `GOOS=linux` and `GOOS=windows` cross runs that the Makefile target performs (the target does this automatically).
- [ ] Run `make verify` (or at minimum `make verify-lint`) to confirm the broader verify chain still passes.
- [ ] Stage `.golangci.yml` only. Commit message body should briefly note the rule blocks `log.Fatal*`, `logrus.Fatal*`, `klog.Fatal*` to prevent regression of the PR #2106 cleanup, and that test files are exempted.
- [ ] Tests: none — config-only change validated by lint runs in Tasks 1 and 2.

## Questions

1. **Scope of the rule** — should the linter apply repo-wide (excluding `vendor/` and `_test.go`) or only to `cmd/crictl/`?
   - Option A: Repo-wide, with `_test.go` exempt. Simplest and consistent with intent — no production code anywhere in the repo should use `Fatal*` for control flow.
   - Option B: Path-scope to `cmd/crictl/` only, matching the literal scope of PR #2106.
   - Suggested: **Option A** — `pkg/` and `cmd/critest/` are part of the same binary set and the same convention should apply; the regression risk is identical.

2. **Which Fatal families to block** — the cleaned-up PR only touched `logrus.Fatal*` and `log.Fatalf`. Should we also preemptively block `klog.Fatal*`?
   - Option A: Block `logrus.Fatal*`, `log.Fatal*`, and `klog.Fatal*`.
   - Option B: Block only the two that were actually present (`logrus.Fatal*`, `log.Fatal*`).
   - Suggested: **Option A** — `klog` is a transitive dependency (`vendor/k8s.io/klog/v2`) and could plausibly be imported in the future; covering it now costs nothing.

3. **Linter choice** — the task body mentions "golangci linter or any other means". I recommend `forbidigo` (already listed-but-disabled in `.golangci.yml`).
   - Option A: `forbidigo` regex patterns inside the existing golangci-lint config.
   - Option B: A custom `hack/verify-no-fatal.sh` script wired into `make verify`.
   - Suggested: **Option A** — zero new tooling, runs in the same CI step that already gates PRs, and IDE integrations surface findings inline.
