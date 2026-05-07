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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"

	"sigs.k8s.io/cri-tools/pkg/common"
	"sigs.k8s.io/cri-tools/pkg/framework"
)

var _ = framework.KubeDescribe("NRI", func() {
	f := framework.NewDefaultCRIFramework()

	var rc internalapi.RuntimeService

	BeforeEach(func() {
		if framework.TestContext.NRISocketPath == "" {
			Skip("NRI socket not configured (use -nri-socket flag)")
		}

		rc = f.CRIClient.CRIRuntimeClient
	})

	Context("runtime should invoke NRI RunPodSandbox hook on pod creation", Serial, func() {
		var (
			testStub  *NRITestStub
			podID     string
			podConfig *runtimeapi.PodSandboxConfig
		)

		BeforeEach(func() {
			var err error

			testStub, err = StartNRITestStub("cri-test-nri", "00")
			Expect(err).NotTo(HaveOccurred(), "failed to start NRI test stub")
		})

		AfterEach(func(ctx SpecContext) {
			if podID != "" {
				By("stop PodSandbox")
				Expect(rc.StopPodSandbox(ctx, podID)).NotTo(HaveOccurred())
				By("remove PodSandbox")
				Expect(rc.RemovePodSandbox(ctx, podID)).NotTo(HaveOccurred())
			}

			if testStub != nil {
				testStub.Cleanup()
			}
		})

		It("should receive RunPodSandbox event when a pod is created via CRI", func(ctx SpecContext) {
			By("creating a pod sandbox")

			podSandboxName := "nri-test-run-hook-" + framework.NewUUID()
			uid := framework.DefaultUIDPrefix + framework.NewUUID()
			namespace := framework.DefaultNamespacePrefix + framework.NewUUID()
			podConfig = &runtimeapi.PodSandboxConfig{
				Metadata: framework.BuildPodSandboxMetadata(podSandboxName, uid, namespace, framework.DefaultAttempt),
				Linux: &runtimeapi.LinuxPodSandboxConfig{
					CgroupParent: common.GetCgroupParent(ctx, rc),
				},
				Labels: framework.DefaultPodLabels,
			}

			podID = framework.RunPodSandbox(ctx, rc, podConfig)
			Expect(podID).NotTo(BeEmpty())

			By("waiting for RunPodSandbox NRI event")

			event, err := testStub.Plugin.WaitForEvent(EventRunPodSandbox, 10*time.Second)
			Expect(err).NotTo(HaveOccurred(), "NRI stub did not receive RunPodSandbox event")
			Expect(event.PodName).To(Equal(podSandboxName))
		})
	})
})
