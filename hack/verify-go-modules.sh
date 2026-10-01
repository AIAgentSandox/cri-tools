#!/usr/bin/env bash

# Copyright 2017 The Kubernetes Authors.
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

set -e

export GO111MODULE=on
source "$(dirname "${BASH_SOURCE[0]}")/lib/generated.sh"

regenerate() {
    go mod tidy && go mod vendor && go mod verify
}

GENERATED_PATHS=(go.mod go.sum vendor)
verify_generated regenerate

# Test if we can resolve all go modules
go list -mod=readonly -m all
