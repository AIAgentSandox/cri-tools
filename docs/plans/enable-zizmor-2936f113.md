# Enable zizmor GitHub Actions Linter

## Overview
Wire the `zizmor` GitHub Actions static analyzer into both CI and local
development using the same pattern as existing tools (Makefile-installed
binary, `verify-` target, dependency tracking in `dependencies.yaml`), then
fix every finding zizmor currently reports against the repo's workflow
files.

## Context

- Files involved:
  - `Makefile` — add `ZIZMOR_VERSION`, `$(ZIZMOR)` path var, install
    recipe, `verify-zizmor` target, `install.zizmor` target, and include
    `verify-zizmor` in the aggregate `verify` target.
  - `dependencies.yaml` — track the new `zizmor` pin so
    `verify-dependencies` (zeitgeist) keeps it in sync with the Makefile.
  - `.github/workflows/build.yml` — add `zizmor` to the existing
    `linters` matrix (it already runs `make verify-${{ matrix.run }}`)
    so the local `make verify-zizmor` target is exercised in CI. Also
    fix `cache-poisoning` (3 × setup-go) and `artipacked` (6 × checkout)
    findings.
  - `.github/workflows/containerd.yml` — fix `cache-poisoning` on the
    `actions/cache` step (line 96), `template-injection` on lines
    381/403/404, and `artipacked` on the checkout at line 30 (plus any
    other unannotated checkouts in the file).
  - `.github/workflows/crio.yml` — fix `cache-poisoning` on setup-go
    (line 36) and `artipacked` on checkout (line 32).
  - `.github/workflows/release.yml` — fix `cache-poisoning` on setup-go
    (line 19), `artipacked` on checkout (line 16), and
    `superfluous-actions` (line 25) by replacing
    `ncipollo/release-action` with `gh release create`.

- Related patterns:
  - Tool install pattern at `Makefile:51-54` (`curl_to`) and
    `Makefile:200-201` (`$(ZEITGEIST)` recipe). zizmor ships as a
    `.tar.gz` containing a single `zizmor` binary, so the recipe needs
    `tar -xz` rather than a plain `curl_to`.
  - Verify target convention at `Makefile:145-198`
    (`verify`, `verify-lint`, `verify-dependencies` …).
  - Linters matrix at `.github/workflows/build.yml:22-41` —
    `make verify-${{ matrix.run }}` lets a new matrix entry trigger
    a new verify target with no extra job wiring.
  - SHA-pinned third-party actions, e.g.
    `actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2`.
  - `containerd.yml:72` already uses `cache: false` on setup-go — that
    is the pattern to copy to the other workflows.
  - `release.yml:8-9` already restricts top-level `permissions:` to
    `contents: read`; the `publish` job will need `contents: write`
    locally if it uses `gh release create`.

- Dependencies: `zizmor` v1.25.2 (latest stable, repo
  `zizmorcore/zizmor`). Asset URL pattern:
  `https://github.com/zizmorcore/zizmor/releases/download/v<VERSION>/zizmor-<TARGET>.tar.gz`.
  For Makefile use on Linux amd64 the asset is
  `zizmor-x86_64-unknown-linux-gnu.tar.gz`; no external runtime
  dependency.

## Development Approach

- **Testing approach**: Follow the repository's existing testing
  practices — `make verify-zizmor` must exit 0, and the new workflow
  matrix entry must succeed under the project's existing CI. There is
  no Go-level test coverage for Makefile or workflow files in this
  repo; do not invent one.
- Complete each task fully before moving to the next.
- **CRITICAL: all tests must pass before starting next task.** After
  each task run `make verify-zizmor` (once it exists) plus
  `make verify` to confirm nothing else regresses.

## Implementation Steps

### Task 1: Add zizmor install + verify plumbing to the Makefile

Add the version constant, binary path variable, download recipe, and
verify target. Use the same idioms as the existing `$(ZEITGEIST)` setup
but adapt for a tarball asset.

**Files:**
- Modify: `Makefile`

- [x] Add `ZIZMOR_VERSION := v1.25.2` near the other tool-version
      constants around `Makefile:56-59`.
- [x] Add `ZIZMOR := $(BUILD_BIN_PATH)/zizmor` near the other binary
      path vars around `Makefile:61-65`.
- [x] Add a `$(ZIZMOR): $(BUILD_BIN_PATH)` recipe that detects host
      `GOOS`/`GOARCH`, maps them to the Rust target triple (Linux:
      `x86_64-unknown-linux-gnu` / `aarch64-unknown-linux-gnu`, macOS:
      `x86_64-apple-darwin` / `aarch64-apple-darwin`), `curl`s the
      release tarball to a temp file with the same retry flags used in
      `curl_to`, extracts only the `zizmor` binary into
      `$(BUILD_BIN_PATH)`, and `chmod +x`es it. Place this near the
      `$(ZEITGEIST)` recipe.
- [x] Add `.PHONY: verify-zizmor` plus a target
      `verify-zizmor: $(ZIZMOR) ## Run zizmor on .github/workflows/.`
      that runs `$(ZIZMOR) .github/workflows/` (the default
      `min-severity` of low matches the findings list in the task).
- [x] Add `.PHONY: install.zizmor` plus
      `install.zizmor: $(ZIZMOR) ## Install zizmor.` mirroring
      `install.lint`.
- [x] Extend the aggregate `verify:` target at `Makefile:146` to
      include `verify-zizmor`.
- [x] Write/update tests (if the repository has test coverage for
      similar code) — N/A; Makefile changes are validated by running
      `make verify-zizmor` and confirming it builds the binary and
      exits 0 once Task 4 lands.

### Task 2: Track zizmor in `dependencies.yaml`

Make `verify-dependencies` (zeitgeist) own the version pin so future
bumps are picked up consistently.

**Files:**
- Modify: `dependencies.yaml`

- [x] Add a `zizmor` entry with `version: v1.25.2` and a `refPaths`
      entry pointing to `Makefile` matching `ZIZMOR_VERSION` — same
      shape as the existing `zeitgeist`/`golangci-lint` entries.
- [x] Run `make verify-dependencies` and confirm it passes.

### Task 3: Add zizmor to the build workflow's `linters` matrix

The existing `linters` job runs `make verify-${{ matrix.run }}` per
matrix entry, so the only change needed is one new matrix value. This
keeps CI and local invocation on the same code path. (The checkout/
setup-go findings on this same job are fixed in Task 4.)

**Files:**
- Modify: `.github/workflows/build.yml`

- [x] Add `- zizmor` to the matrix `run:` list at
      `build.yml:28-33`.
- [x] Confirm `make verify-zizmor` is what the new matrix entry will
      invoke and that no extra steps (e.g. an apt package) are needed
      on `ubuntu-latest` — the Makefile recipe downloads a static
      binary.

### Task 4: Fix `cache-poisoning` findings (4 workflows)

zizmor flags `actions/setup-go` because its built-in module cache can
be poisoned by a malicious PR head. Disable the built-in cache; do
*not* re-introduce caching here because none of these jobs currently
rely on warm Go caches and the only file already using
`actions/cache` (containerd.yml) needs a separate fix (next bullet).

**Files:**
- Modify: `.github/workflows/build.yml`, `.github/workflows/crio.yml`,
  `.github/workflows/release.yml`, `.github/workflows/containerd.yml`

- [x] `build.yml:38`, `build.yml:50`, `build.yml:71` — add
      `cache: false` under the existing `with:` block of each
      `actions/setup-go` step (copy the pattern already used at
      `containerd.yml:72`).
- [x] `crio.yml:36` — add `cache: false` under that step's `with:`.
- [x] `release.yml:19` — add `cache: false` under that step's `with:`.
- [x] `containerd.yml:95-102` — convert the `actions/cache` step to
      restore-only on PRs: add
      `lookup-only: ${{ github.event_name == 'pull_request' }}` (or
      equivalent guard) so untrusted PR runs cannot overwrite the
      shared cache. Match the fix wording in the task description.
      (Implemented as `if: github.event_name != 'pull_request'` plus a
      `zizmor: ignore[cache-poisoning]` inline comment because zizmor's
      static analysis does not recognize the `if:` mitigation.)
- [x] Re-run `make verify-zizmor` and confirm all
      `cache-poisoning` findings are gone.

### Task 5: Fix `template-injection` in `containerd.yml`

`${{ env.CONTD_CRI_DIR }}` is expanded by GitHub *before* the shell
sees the script, so a tainted env value can inject shell. The fix is
to dereference the env var inside the shell. (Same value is set by an
earlier `echo "CONTD_CRI_DIR=..." >> $GITHUB_ENV` step, so the env
mapping continues to work for the `actions/upload-artifact` `path:`
input on line 388.)

**Files:**
- Modify: `.github/workflows/containerd.yml`

- [ ] Around `containerd.yml:381` — the
      `actions/upload-artifact` step's `path:` is an action input, not
      a `run:` block; if zizmor still flags it after the other fixes,
      add `env: { CONTD_CRI_DIR: ${{ env.CONTD_CRI_DIR }} }` on the
      step and reference `$CONTD_CRI_DIR` from `path:` only if
      supported, otherwise leave the `path:` expression alone and add
      a zizmor inline ignore comment scoped to that single line with a
      short justification. Verify the fix matches what zizmor accepts.
- [ ] `containerd.yml:393-397` (Linux cleanup `run:` block) —
      replace `${{env.CONTD_CRI_DIR}}` with `"$CONTD_CRI_DIR"` in both
      the `echo` and `sudo rm -rf` lines.
- [ ] `containerd.yml:399-404` (Windows cleanup `run:` block) — same
      replacement, ensuring the bash shell semantics still work under
      `shell: bash` on Windows.
- [ ] Re-run `make verify-zizmor` and confirm all
      `template-injection` findings are gone.

### Task 6: Fix `artipacked` findings (9 checkouts)

Each `actions/checkout` step persists the `GITHUB_TOKEN` into the
local `.git/config` by default; zizmor's `artipacked` rule wants
checkouts that don't need to push to set `persist-credentials: false`.
None of the listed jobs push back to git via the local checkout.

**Files:**
- Modify: `.github/workflows/build.yml`,
  `.github/workflows/containerd.yml`, `.github/workflows/crio.yml`,
  `.github/workflows/release.yml`

- [ ] `build.yml:19,35,47,66,80,88` — add `with: { persist-credentials: false }`
      to each checkout step. For checkouts that already have a `with:`
      block (e.g. line 66 has `fetch-depth: 0`, line 35 may merge with
      existing keys) extend that block instead of creating a duplicate.
- [ ] `containerd.yml:30` (and any other unannotated checkouts in
      the file — there are several inside the same job) — add
      `persist-credentials: false` to each. The
      `containerd/containerd` and `Microsoft/hcsshim` checkouts are
      external repos, so the option is even more clearly correct
      there.
- [ ] `crio.yml:32` — add `persist-credentials: false` to the
      checkout step.
- [ ] `release.yml:16` — add `persist-credentials: false` under the
      existing `with:` block that holds `fetch-depth: 0`.
- [ ] Re-run `make verify-zizmor` and confirm all
      `artipacked` findings are gone.

### Task 7: Replace `ncipollo/release-action` with built-in `gh release create`

zizmor's `superfluous-actions` rule notes that this third-party
release-creation action duplicates what `gh release` already does.
Switching reduces the supply-chain surface and uses an action-less
shell step.

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] Replace the `- uses: ncipollo/release-action@…` block at
      `release.yml:25-30` with a `run:` step that calls
      `gh release create "$GITHUB_REF_NAME" _output/releases/* --notes-file release-notes.md --target "$GITHUB_SHA"`
      (use `--clobber` semantics via `gh release upload` if updates to
      an existing tag are required to match the previous
      `allowUpdates: true` behavior — see Question 1).
- [ ] Add `permissions: contents: write` at the job level on the
      `publish` job so `gh release create` can write — keep the
      top-level `contents: read` default.
- [ ] Set `GH_TOKEN: ${{ secrets.GH_TOKEN }}` on the step env so
      `gh` authenticates with the same token the previous action used
      (the file comment at `release.yml:6-7` already notes this).
- [ ] Re-run `make verify-zizmor` and confirm the
      `superfluous-actions` finding is gone.

### Task 8: Final verification

Make sure everything ties together and update the `dependencies.yaml`
pin check is happy.

**Files:**
- Modify: none (verification only).

- [ ] Run `make verify-zizmor` — must exit 0.
- [ ] Run `make verify-dependencies` — must exit 0.
- [ ] Run `make verify` — must exit 0 (this now also runs
      `verify-zizmor`).
- [ ] Skim the diff of the four workflow files and confirm no
      semantic behavior change beyond the zizmor fixes (e.g. checkout
      `fetch-depth`, action SHAs, conditionals all intact).

## Questions

1. The current release flow uses
   `ncipollo/release-action` with `allowUpdates: true`, which lets a
   re-run on the same tag update the existing release. `gh release create`
   fails if the release exists; the idiomatic replacement is a two-step
   `gh release view "$TAG" || gh release create …` plus
   `gh release upload "$TAG" _output/releases/* --clobber`. Which
   behavior should we preserve?
   - Option A: Preserve update-on-existing semantics with the two-step
     view/create + upload --clobber pattern. (Recommended — matches
     current behavior, lets re-runs succeed.)
   - Option B: Switch to strict create-only (`gh release create`
     unconditionally). Simpler, but a re-run of the workflow on the
     same tag will fail.
   - Suggested: Option A.

2. The Makefile already has `curl_to` for single-file downloads, but
   zizmor's release asset is a tarball. Should the new download
   recipe be a one-off inline shell block in the `$(ZIZMOR)` target,
   or should we generalize a second helper like `curl_tar_to`?
   - Option A: Inline shell block inside `$(ZIZMOR)` only. (Recommended
     — zizmor is the only current tarball-based tool, so YAGNI.)
   - Option B: Add a `curl_tar_to` macro alongside `curl_to`.
   - Suggested: Option A.

3. Should `verify-zizmor` run with `--min-severity=low` (matches
   today's finding set so regressions of any flavor break CI) or a
   higher threshold (less noisy, but lets low/info regressions slip
   in)?
   - Option A: Default severity (low) — fail on anything zizmor
     reports. (Recommended — once we've cleaned the slate this keeps
     it clean.)
   - Option B: `--min-severity=medium` — only fail on medium+; rely on
     log inspection for low/info.
   - Suggested: Option A.
