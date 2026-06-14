# container lifecycle failed container

## Implementation Steps

### Task 1: container lifecycle failed container

- [x] Implement the similar test as https://github.com/kubernetes-sigs/cri-tools/pull/2118 but the container needs to fail. The check will verify which NRI callbacks will be called. Use `make test-critest-containerd` to verify it works (implemented as the "container lifecycle (failed container)" Context in pkg/validate/nri_linux.go; verified via go build/vet/test-compile. `make test-critest-containerd` requires a live containerd+NRI runtime — not automatable in this environment.)
