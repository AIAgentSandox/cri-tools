#!/usr/bin/env bash

# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Helpers to verify that generated files are up to date.
#
# On a clean working tree the whole tree must stay clean after regeneration,
# so the committed state is what gets verified. On a dirty working tree only
# the paths in GENERATED_PATHS are checked, so verification works while there
# are uncommitted changes.

# snapshot_paths prints a git tree hash of the working tree content of the
# given paths, including uncommitted and untracked files. It works on a copy
# of the index in SNAPSHOT_INDEX, so the real index is left untouched.
# Errors are returned explicitly because errexit is not inherited by command
# substitution.
snapshot_paths() {
    local real_index
    real_index=$(git rev-parse --git-path index) || return 1
    cp "$real_index" "$SNAPSHOT_INDEX" || return 1
    GIT_INDEX_FILE="$SNAPSHOT_INDEX" git add -A -- "$@" || return 1
    GIT_INDEX_FILE="$SNAPSHOT_INDEX" git write-tree
}

# verify_generated runs the given command and fails if it modified the tree:
# any file when the tree was clean, otherwise the paths in GENERATED_PATHS.
verify_generated() {
    local status
    status=$(git status --porcelain) || exit 1
    if [[ -z "$status" ]]; then
        verify_generated_clean "$@"
    else
        verify_generated_dirty "$@"
    fi
}

# verify_generated_clean runs the given command and fails if the working tree
# is no longer clean afterwards.
verify_generated_clean() {
    local status
    "$@"
    status=$(git status --porcelain) || exit 1
    if [[ -z "$status" ]]; then
        echo "tree is clean"
    else
        echo "tree is dirty, please commit all changes"
        echo ""
        echo "$status"
        exit 1
    fi
}

# verify_generated_dirty runs the given command and fails if it modified any
# of the paths listed in GENERATED_PATHS.
verify_generated_dirty() {
    local before after
    SNAPSHOT_INDEX=$(mktemp) || exit 1
    trap 'rm -f "$SNAPSHOT_INDEX"' EXIT

    before=$(snapshot_paths "${GENERATED_PATHS[@]}") || exit 1
    "$@"
    after=$(snapshot_paths "${GENERATED_PATHS[@]}") || exit 1

    # Only compare the generated paths: the rest of the trees comes from the
    # real index, which may change while the command runs.
    if git diff --quiet "$before" "$after" -- "${GENERATED_PATHS[@]}"; then
        echo "generated files are up to date"
    else
        echo "generated files were out of date and have been regenerated:"
        echo ""
        git diff --stat "$before" "$after" -- "${GENERATED_PATHS[@]}"
        exit 1
    fi
}
