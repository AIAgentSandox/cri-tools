# Align Local Containerized Test (`test-critest-containerd`) with CI

## Overview
The local containerized critest (`make test-critest-containerd`) currently installs containerd from the Ubuntu apt repository inside a Docker image tagged `containerd-local-test:latest`. This means: (1) there is no way to pick a containerd version, (2) the version drifts from what CI actually exercises (CI builds containerd from source at the `main` and `release/1.7` git refs), and (3) the single `:latest` tag prevents per-version caching, so changing anything forces a full rebuild.

This plan parameterizes the containerd version, builds containerd from source inside the image (matching how CI installs it), embeds the version in the image tag so per-version images are cached and reused across runs, reuses containerd's own setup scripts (as CI does) to stay aligned, and adds a clean Makefile target to delete the generated images/volumes so they can be regenerated.

## Context
- **Files involved:**
  - Modify: `images/containerd-local-test/Dockerfile` — multi-stage build that compiles containerd from source at a configurable ref, plus runc/crun and CNI via containerd's own scripts.
  - Modify: `hack/run-e2e-container.sh` — accept `CONTAINERD_VERSION` / `RUNC_FLAVOR` / `RUNTIME`, pass as build args, tag the image with the (sanitized) containerd version, and reuse the cached image when present.
  - Modify: `hack/setup-containerd.sh` — make NRI configuration conditional (CI only enables NRI for `main`, not `release/1.7`) and keep config aligned with the CI `config.toml`.
  - Modify: `Makefile` — expose the containerd version variable to `test-critest-containerd` / `test-crictl-e2e-containerd`, conditionally pass `--nri-socket`/`--parallel`, and add a `clean-critest-containerd-images` target.
  - Possibly modify: `images/containerd-local-test/entrypoint.sh` — only relevant if containerd start flags need to change for source builds.
  - Reference (read-only, do not modify): `.github/workflows/containerd.yml` — the source of truth for CI behavior to mirror.
- **Related patterns:**
  - CI installs containerd via `make && make install` from a checkout of `containerd/containerd` at `ref` (`main` or `release/1.7`) — `.github/workflows/containerd.yml:120-133`.
  - CI installs runc/crun via containerd's `script/setup/install-runc` with `RUNC_FLAVOR` — `.github/workflows/containerd.yml:255-261`.
  - CI installs CNI via containerd's `script/setup/install-cni` (version-specific branch) — `.github/workflows/containerd.yml:135-148`.
  - CI enables NRI and passes `--nri-socket` only for `version == main` — `.github/workflows/containerd.yml:317-327,344-346`.
  - CI runs critest with `--ginkgo.v --parallel=8` — `.github/workflows/containerd.yml:342`.
  - Existing image tag convention `containerd-local-test:latest` lives in `hack/run-e2e-container.sh:34`; the named data volume `containerd-local-test-data` in `hack/run-e2e-container.sh:64`.
- **Dependencies:** Docker (already required), Go toolchain in the build stage (containerd source build needs Go + `libseccomp-dev`, `libbtrfs-dev`, `btrfs-progs`, build tools — same packages CI installs at `.github/workflows/containerd.yml:109-118`). No new host-side dependencies.

## Development Approach

- **Testing approach**: This area has no unit-test coverage — it is shell/Docker/Makefile infrastructure. Follow the repo's existing practice: validate by actually running the targets (`make test-critest-containerd`, the new clean target) and confirming behavior. There is no test harness to add; do not invent one. Where practical, run `shellcheck` / `make verify` if the repo already runs those (check `Makefile` verify targets) so the scripts stay lint-clean.
- Complete each task fully before moving to the next.
- **CRITICAL: all tests must pass before starting the next task** — for this infra work, "pass" means the target runs end-to-end (image builds, containerd boots, critest starts) for the affected change.

## Implementation Steps

### Task 1: Parameterize containerd version and build it from source in the image
Replace the apt-installed containerd with a source build at a configurable git ref, mirroring how CI installs containerd. Use a multi-stage Dockerfile so the build toolchain does not bloat the final image.

**Files:**
- Modify: `images/containerd-local-test/Dockerfile`

- [ ] Add a `CONTAINERD_VERSION` build `ARG` (default `main`) and a `RUNC_FLAVOR` build `ARG` (default `runc`, allowing `crun`), matching the CI matrix values (`main`, `release/1.7`; `runc`, `crun`).
- [ ] Add a builder stage (e.g. `FROM golang:<version matching go.mod> AS containerd-builder`) that installs the same build deps CI uses (`btrfs-progs`, `libbtrfs-dev`, `libseccomp2`, `libseccomp-dev`, `socat`, build-essential, git), `git clone`s `https://github.com/containerd/containerd` and checks out `${CONTAINERD_VERSION}`, then runs `make && make install` (mirrors `.github/workflows/containerd.yml:120-133`).
- [ ] In the builder stage, install runc/crun by running containerd's `script/setup/install-runc` with `RUNC_FLAVOR=${RUNC_FLAVOR}`, and install CNI plugins via containerd's `script/setup/install-cni` (use the version-specific invocation: for `release/1.7` call it with no arg, otherwise pass the `containernetworking/plugins` version from `go.mod`, per `.github/workflows/containerd.yml:135-148`). This replaces the hardcoded `CNI_PLUGINS_VERSION=v1.4.0` curl download so CNI matches the containerd ref under test.
- [ ] In the final runtime stage (keep `ubuntu:26.04` or a slim base), install only runtime deps (`apparmor-utils`, `ca-certificates`, `iptables`, `libseccomp2`, `socat`, `sudo`) and `COPY --from=containerd-builder` the containerd binaries, runc/crun, and CNI plugins (`/opt/cni/bin`).
- [ ] Keep copying `hack/setup-containerd.sh`, `hack/wait-for-containerd.sh`, and `images/containerd-local-test/entrypoint.sh`, and keep `ENV PATH="/usr/local/bin/critest-tools:${PATH}"` and the entrypoint.
- [ ] Confirm `images/containerd-local-test/Dockerfile.dockerignore` still allows the needed build context (the Dockerfile no longer needs anything new from context beyond the existing scripts).
- [ ] Verify: `docker build --build-arg CONTAINERD_VERSION=main -f images/containerd-local-test/Dockerfile .` succeeds and `containerd --version` inside the image reports the expected ref.
- [ ] No automated tests exist for this file; verification is the successful build above.

### Task 2: Embed containerd version in the image tag and reuse the cached image
Make `run-e2e-container.sh` build/tag a per-version image so it is cached and reused on subsequent runs, only rebuilding when the requested version (or runc flavor) changes.

**Files:**
- Modify: `hack/run-e2e-container.sh`

- [ ] Read `CONTAINERD_VERSION` (default `main`), `RUNC_FLAVOR` (default `runc`), and keep the existing `RUNTIME` (default `io.containerd.runc.v2`) env vars near the top of the script.
- [ ] Sanitize the version for use in a Docker tag (e.g. replace `/` with `-`, so `release/1.7` → `release-1.7`) and build `IMAGE_NAME="containerd-local-test:${SANITIZED_VERSION}"` (and optionally include `-${RUNC_FLAVOR}` when not `runc`) to replace the fixed `:latest` tag at `hack/run-e2e-container.sh:34`.
- [ ] Pass `--build-arg CONTAINERD_VERSION=${CONTAINERD_VERSION} --build-arg RUNC_FLAVOR=${RUNC_FLAVOR}` to `docker build`.
- [ ] Skip the rebuild when the tagged image already exists locally unless a `FORCE_REBUILD`/`REBUILD` env is set — e.g. guard the `docker build` with `docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1` so a cached per-version image is reused (this is the "cached and reused for next test execution" requirement). Rely on Docker layer cache for the version-change case.
- [ ] Pass `RUNTIME` through to the container run (already done at `hack/run-e2e-container.sh:62`); keep the named data volume but consider namespacing it per version (e.g. `containerd-local-test-data-${SANITIZED_VERSION}`) so image data from different containerd versions does not collide — document this choice in the script comment.
- [ ] Verify: run the script twice with the same `CONTAINERD_VERSION` and confirm the second run skips the build; run with a different version and confirm a new tagged image is produced.
- [ ] No automated tests for this script; verification is the runs above.

### Task 3: Align containerd config / NRI / critest flags with CI behavior
Keep the in-container configuration as close to CI as possible, including making NRI conditional on the version (CI enables NRI only for `main`).

**Files:**
- Modify: `hack/setup-containerd.sh`
- Modify: `Makefile` (test target flags)
- Possibly modify: `images/containerd-local-test/entrypoint.sh`

- [ ] In `hack/setup-containerd.sh`, gate the NRI plugin block (`hack/setup-containerd.sh:38-47`) behind an env flag (e.g. `ENABLE_NRI`, default derived from version) so it is only emitted when the containerd version supports/needs it — matching CI which adds NRI config only for `main` (`.github/workflows/containerd.yml:317-327`). Pass `ENABLE_NRI` from `run-e2e-container.sh` based on `CONTAINERD_VERSION` (`main` → true; `release/1.7` → false).
- [ ] Confirm the `config.toml` runtime wiring still honors `RUNTIME` for the configured runtime handler (keep current modern CRI plugin layout; note CI's `release/1.7` uses the older `[plugins.cri.containerd.default_runtime]` schema — if `release/1.7` is targeted, branch the config generation to emit the schema that version accepts, otherwise the daemon will reject it).
- [ ] In the `Makefile` `test-critest-containerd` target (`Makefile:220-228`), make `--nri-socket=/var/run/nri/nri.sock` conditional so it is only passed when NRI is enabled (i.e. for `main`), mirroring `.github/workflows/containerd.yml:344-346`. Add a `--parallel` value consistent with CI's `--parallel=8` (allow override via a variable/`TESTFLAGS`).
- [ ] Verify: `make test-critest-containerd CONTAINERD_VERSION=main` boots containerd with NRI and runs critest with `--nri-socket`; a `release/1.7` run boots without NRI and without `--nri-socket`.
- [ ] No automated tests; verification is the two runs above.

### Task 4: Add a clean Makefile target to delete generated images and volumes
Provide a clean way to remove the cached per-version images (and data volumes) so they can be regenerated.

**Files:**
- Modify: `Makefile`

- [ ] Add a `clean-critest-containerd-images` PHONY target (place it near the other test targets, ~`Makefile:238`, or under the Utility section) with a `##` help comment so it shows in `make help`.
- [ ] The target should remove all images matching the `containerd-local-test` repository (e.g. `docker images --filter=reference='containerd-local-test:*' -q | xargs -r docker rmi -f`) and the associated named data volume(s) (`docker volume rm -f containerd-local-test-data*` or the per-version names chosen in Task 2). Guard for the case where none exist so the target does not fail.
- [ ] Verify: build at least one image via `make test-critest-containerd`, then run `make clean-critest-containerd-images` and confirm `docker images` no longer lists `containerd-local-test:*` and the volume is gone.
- [ ] No automated tests; verification is the build-then-clean cycle above.

### Task 5: Wire the version variable through the Makefile and document usage
Expose `CONTAINERD_VERSION` (and `RUNC_FLAVOR`/`RUNTIME`) through the Makefile so users invoke `make test-critest-containerd CONTAINERD_VERSION=release/1.7` cleanly, and document the new behavior.

**Files:**
- Modify: `Makefile`
- Possibly modify: a docs/README section if one references `test-critest-containerd` (search for existing references before adding new docs; do NOT create a new doc file unless one already documents these targets).

- [ ] In the `Makefile`, define `CONTAINERD_VERSION ?= main`, `RUNC_FLAVOR ?= runc`, and reuse the existing `RUNTIME` convention, and export them into the `hack/run-e2e-container.sh` invocation for both `test-critest-containerd` and `test-crictl-e2e-containerd` (`Makefile:220-238`).
- [ ] Update the `##` help text on the containerd test targets to mention the configurable version.
- [ ] Search the repo (`docs/`, `README.md`, `*.md`) for existing mentions of `test-critest-containerd`; if found, update them to describe `CONTAINERD_VERSION`, image caching by version, and the new `clean-critest-containerd-images` target. If no existing doc references it, do not create one.
- [ ] Verify: `make help` shows the updated descriptions; `make test-critest-containerd CONTAINERD_VERSION=main` runs end-to-end.
- [ ] No automated tests; verification is the `make help` output and the end-to-end run.

## Questions

1. **How should containerd be installed in the image?**
   - Option A: Build containerd from source at a git ref (`main`, `release/1.7`), exactly matching CI (`make && make install`, plus containerd's `install-runc`/`install-cni` scripts). Best alignment with CI and supports the same refs CI tests; larger/slower first build (cached afterward via the per-version tag).
   - Option B: Keep apt but pin a released `containerd=<version>` package. Faster build, but versions diverge from CI (CI tests branch tips like `main`/`release/1.7`, not apt releases), and apt may not offer the exact versions.
   - **Suggested: Option A** — the task explicitly asks to "align as much with CI" and "support different containerd versions"; CI builds from source at branch refs, so source build is the only way to truly match.

Answer: Option A - build from sources.

2. **What should the default `CONTAINERD_VERSION` be?**
   - Option A: `main` (matches CI's primary matrix entry and the NRI-enabled path).
   - Option B: `release/1.7` (the LTS-ish branch).
   - **Suggested: Option A (`main`)** — it is the configuration CI exercises most fully (NRI on), so the default local run mirrors the richest CI path.

Answer: Option A - same as CI - main

3. **Should the data volume be namespaced per containerd version?**
   - Option A: Yes — `containerd-local-test-data-<version>` to avoid image-store/schema incompatibilities between containerd versions.
   - Option B: No — keep a single shared `containerd-local-test-data` volume (simpler, but risks corruption when switching versions).
   - **Suggested: Option A** — different containerd versions can use incompatible content/metadata stores, and per-version volumes keep the cache-and-reuse behavior safe; the clean target removes all of them.

Answer: Option A - Yes, per version
