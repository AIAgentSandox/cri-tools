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

# Helpers to verify that generated files are up to date without requiring
# the whole working tree to be clean.

# snapshot_paths prints a git tree hash of the working tree content of the
# given paths, including uncommitted and untracked files. The real index is
# left untouched.
snapshot_paths() {
    local index
    index=$(mktemp)
    cp "$(git rev-parse --git-path index)" "$index"
    GIT_INDEX_FILE="$index" git add -A -- "$@"
    GIT_INDEX_FILE="$index" git write-tree
    rm -f "$index"
}

# verify_generated runs the given command and fails if it modified any of
# the paths listed in GENERATED_PATHS.
verify_generated() {
    local before after
    before=$(snapshot_paths "${GENERATED_PATHS[@]}")
    "$@"
    after=$(snapshot_paths "${GENERATED_PATHS[@]}")

    if [[ "$before" == "$after" ]]; then
        echo "generated files are up to date"
    else
        echo "generated files were out of date and have been regenerated:"
        echo ""
        git diff --stat "$before" "$after"
        exit 1
    fi
}
