/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nri

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/framework"
)

// nriTestPodNamePrefix is the prefix every pod sandbox name built by this package
// shares. Cleanup and leak checks scope their lookups of the stub's recorded events
// by it, so a sandbox created on the node by anyone else is never matched.
const nriTestPodNamePrefix = "nri-test-"

// nriTestClients returns the CRI runtime and image clients of the framework,
// skipping the running spec if no NRI socket has been configured. It has to be
// called from within a Ginkgo node, because the framework only connects its
// clients in its own BeforeEach.
func nriTestClients(
	f *framework.Framework,
) (internalapi.RuntimeService, internalapi.ImageManagerService) {
	if framework.TestContext.NRISocketPath == "" {
		Skip("NRI socket not configured (use -nri-socket flag)")
	}

	return f.CRIClient.CRIRuntimeClient, f.CRIClient.CRIImageClient
}

// getContainerStatus returns the status of the container.
//
// This mirrors the identically named helper in pkg/validate, which stays
// unexported there. Keeping a local copy is deliberate: exporting it would
// require rewriting its ~40 call sites in pkg/validate for the sake of eight
// lines that only wrap ContainerStatus.
func getContainerStatus(
	ctx context.Context,
	c internalapi.RuntimeService,
	containerID string,
) *runtimeapi.ContainerStatus {
	By("Get container status for containerID: " + containerID)
	status, err := c.ContainerStatus(ctx, containerID, false)
	framework.ExpectNoError(err, "failed to get container %q status", containerID)

	return status.GetStatus()
}
