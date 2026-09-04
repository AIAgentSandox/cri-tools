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
	"slices"
	"strings"
	"sync"
	"time"

	nri "github.com/containerd/nri/pkg/api"
)

// NRIEventType represents the type of NRI lifecycle event.
type NRIEventType string

const (
	EventRunPodSandbox    NRIEventType = "RunPodSandbox"
	EventStopPodSandbox   NRIEventType = "StopPodSandbox"
	EventRemovePodSandbox NRIEventType = "RemovePodSandbox"
	EventCreateContainer  NRIEventType = "CreateContainer"
	EventStartContainer   NRIEventType = "StartContainer"
	EventStopContainer    NRIEventType = "StopContainer"
	EventRemoveContainer  NRIEventType = "RemoveContainer"
)

// NRIEvent records an NRI lifecycle event with metadata.
type NRIEvent struct {
	Type          NRIEventType
	PodSandboxID  string
	PodName       string
	PodNamespace  string
	PodUID        string
	ContainerID   string
	ContainerName string
	Timestamp     time.Time
}

// NRITestPlugin implements the NRI plugin interfaces for testing.
// It records all events and supports optional hook callbacks for blocking/error injection.
type NRITestPlugin struct {
	mu     sync.Mutex
	events []NRIEvent

	// ready is closed when Synchronize is called (after register/configure handshake).
	ready     chan struct{}
	readyOnce sync.Once

	// syncPods/syncContainers capture the pod and container IDs passed to the
	// most recent Synchronize call, so tests can assert that a late-joining
	// plugin is reconciled with the runtime's existing state.
	syncPods       []string
	syncContainers []string

	// Hook callbacks - if set, called during the respective hook.
	// Return an error to simulate plugin failure.
	//
	// These are guarded by mu and must only be installed through the SetOn...
	// setters: the stub's request handler goroutine is already live once
	// StartNRITestStub returns, and the runtime may deliver a callback for a
	// sandbox created by another actor on the node at any moment, so a plain
	// field assignment from the spec goroutine races with the handler's read.
	//
	// onSynchronize fires during the Synchronize handshake (before the ready
	// channel is closed), so it must be installed before the stub connects
	// (see the configure callback on StartNRITestStub). Block inside it to hold
	// the Synchronize call open while the test mutates runtime state.
	onSynchronize      SyncHook
	onRunPodSandbox    PodHook
	onStopPodSandbox   PodHook
	onRemovePodSandbox PodHook
	onCreateContainer  ContainerHook
	onStartContainer   ContainerHook
	onStopContainer    ContainerHook
	onRemoveContainer  ContainerHook
}

// Hook callback signatures. Returning an error simulates a plugin failure.
type (
	// SyncHook fires during the Synchronize handshake.
	SyncHook func(ctx context.Context, pods []*nri.PodSandbox, containers []*nri.Container) error
	// PodHook fires during a pod sandbox lifecycle hook.
	PodHook func(ctx context.Context, pod *nri.PodSandbox) error
	// ContainerHook fires during a container lifecycle hook.
	ContainerHook func(ctx context.Context, pod *nri.PodSandbox, container *nri.Container) error
)

// SetOnSynchronize installs the Synchronize hook. It only has an effect when
// called before the stub connects, i.e. from a StartNRITestStub configure
// callback.
func (p *NRITestPlugin) SetOnSynchronize(hook SyncHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onSynchronize = hook
}

// SetOnRunPodSandbox installs the RunPodSandbox hook.
func (p *NRITestPlugin) SetOnRunPodSandbox(hook PodHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onRunPodSandbox = hook
}

// SetOnStopPodSandbox installs the StopPodSandbox hook.
func (p *NRITestPlugin) SetOnStopPodSandbox(hook PodHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onStopPodSandbox = hook
}

// SetOnRemovePodSandbox installs the RemovePodSandbox hook.
func (p *NRITestPlugin) SetOnRemovePodSandbox(hook PodHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onRemovePodSandbox = hook
}

// SetOnCreateContainer installs the CreateContainer hook.
func (p *NRITestPlugin) SetOnCreateContainer(hook ContainerHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onCreateContainer = hook
}

// SetOnStartContainer installs the StartContainer hook.
func (p *NRITestPlugin) SetOnStartContainer(hook ContainerHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onStartContainer = hook
}

// SetOnStopContainer installs the StopContainer hook.
func (p *NRITestPlugin) SetOnStopContainer(hook ContainerHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onStopContainer = hook
}

// SetOnRemoveContainer installs the RemoveContainer hook.
func (p *NRITestPlugin) SetOnRemoveContainer(hook ContainerHook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onRemoveContainer = hook
}

// Synchronize implements stub.SynchronizeInterface.
// It captures the existing pods/containers the runtime reconciles the plugin
// with, then signals readiness (registration/configuration complete) via the
// ready channel.
func (p *NRITestPlugin) Synchronize(
	ctx context.Context,
	pods []*nri.PodSandbox,
	containers []*nri.Container,
) ([]*nri.ContainerUpdate, error) {
	p.mu.Lock()

	p.syncPods = make([]string, 0, len(pods))
	for _, pod := range pods {
		p.syncPods = append(p.syncPods, pod.GetId())
	}

	p.syncContainers = make([]string, 0, len(containers))
	for _, container := range containers {
		p.syncContainers = append(p.syncContainers, container.GetId())
	}

	hook := p.onSynchronize

	p.mu.Unlock()

	// Invoke the optional hook while still inside the Synchronize call. A test
	// may block here to keep the late-joining plugin in its synchronization
	// window (the runtime has not yet observed the plugin as ready) while it
	// creates additional containers, exercising the race where a container
	// created during Synchronize must not be lost.
	if hook != nil {
		if err := hook(ctx, pods, containers); err != nil {
			return nil, err
		}
	}

	p.readyOnce.Do(func() { close(p.ready) })

	return nil, nil
}

// SyncedPods returns the pod sandbox IDs passed to the most recent Synchronize call.
func (p *NRITestPlugin) SyncedPods() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]string, len(p.syncPods))
	copy(result, p.syncPods)

	return result
}

// SyncedContainers returns the container IDs passed to the most recent Synchronize call.
func (p *NRITestPlugin) SyncedContainers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]string, len(p.syncContainers))
	copy(result, p.syncContainers)

	return result
}

// RunPodSandbox implements stub.RunPodInterface.
func (p *NRITestPlugin) RunPodSandbox(ctx context.Context, pod *nri.PodSandbox) error {
	p.recordPodEvent(EventRunPodSandbox, pod)

	hook := p.onRunPodSandboxHook()
	if hook != nil {
		return hook(ctx, pod)
	}

	return nil
}

// StopPodSandbox implements stub.StopPodInterface.
func (p *NRITestPlugin) StopPodSandbox(ctx context.Context, pod *nri.PodSandbox) error {
	p.recordPodEvent(EventStopPodSandbox, pod)

	hook := p.onStopPodSandboxHook()
	if hook != nil {
		return hook(ctx, pod)
	}

	return nil
}

// RemovePodSandbox implements stub.RemovePodInterface.
func (p *NRITestPlugin) RemovePodSandbox(ctx context.Context, pod *nri.PodSandbox) error {
	p.recordPodEvent(EventRemovePodSandbox, pod)

	hook := p.onRemovePodSandboxHook()
	if hook != nil {
		return hook(ctx, pod)
	}

	return nil
}

// CreateContainer implements stub.CreateContainerInterface.
func (p *NRITestPlugin) CreateContainer(
	ctx context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
) (*nri.ContainerAdjustment, []*nri.ContainerUpdate, error) {
	p.recordContainerEvent(EventCreateContainer, pod, container)

	hook := p.onCreateContainerHook()
	if hook != nil {
		if err := hook(ctx, pod, container); err != nil {
			return nil, nil, err
		}
	}

	return nil, nil, nil
}

// StartContainer implements stub.StartContainerInterface.
func (p *NRITestPlugin) StartContainer(
	ctx context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
) error {
	p.recordContainerEvent(EventStartContainer, pod, container)

	hook := p.onStartContainerHook()
	if hook != nil {
		return hook(ctx, pod, container)
	}

	return nil
}

// StopContainer implements stub.StopContainerInterface.
func (p *NRITestPlugin) StopContainer(
	ctx context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
) ([]*nri.ContainerUpdate, error) {
	p.recordContainerEvent(EventStopContainer, pod, container)

	hook := p.onStopContainerHook()
	if hook != nil {
		if err := hook(ctx, pod, container); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// RemoveContainer implements stub.RemoveContainerInterface.
func (p *NRITestPlugin) RemoveContainer(
	ctx context.Context,
	pod *nri.PodSandbox,
	container *nri.Container,
) error {
	p.recordContainerEvent(EventRemoveContainer, pod, container)

	hook := p.onRemoveContainerHook()
	if hook != nil {
		return hook(ctx, pod, container)
	}

	return nil
}

// Events returns a copy of all recorded events.
func (p *NRITestPlugin) Events() []NRIEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]NRIEvent, len(p.events))
	copy(result, p.events)

	return result
}

// Reset clears all recorded events.
func (p *NRITestPlugin) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = nil
	p.syncPods = nil
	p.syncContainers = nil
}

// LastRunPodSandboxID returns the pod sandbox ID from the most recent RunPodSandbox
// event whose pod name starts with namePrefix, or empty string if none recorded.
// This is useful for cleanup in AfterEach when the test may have failed before
// capturing the pod ID from the CRI call.
//
// The prefix is required rather than optional: the plugin observes every sandbox the
// runtime reports while the stub is connected, including sandboxes created by other
// actors on the node (a kubelet, say). Callers use the returned ID for destructive
// cleanup or leak assertions, so an unscoped "most recent" lookup could remove or
// fail on a sandbox this suite never created.
func (p *NRITestPlugin) LastRunPodSandboxID(namePrefix string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range slices.Backward(p.events) {
		if p.events[i].Type == EventRunPodSandbox &&
			strings.HasPrefix(p.events[i].PodName, namePrefix) {
			return p.events[i].PodSandboxID
		}
	}

	return ""
}

// FilterEventsByPodID returns events matching a specific pod sandbox ID.
func FilterEventsByPodID(events []NRIEvent, podID string) []NRIEvent {
	var filtered []NRIEvent

	for i := range events {
		if events[i].PodSandboxID == podID {
			filtered = append(filtered, events[i])
		}
	}

	return filtered
}

// The on...Hook accessors read the installed hook under mu, so the request
// handler goroutine never races with a spec installing a hook. The hook itself
// is invoked after the lock is released: hooks block for the duration of a test
// handshake, and holding mu across one would deadlock event recording.
func (p *NRITestPlugin) onRunPodSandboxHook() PodHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onRunPodSandbox
}

func (p *NRITestPlugin) onStopPodSandboxHook() PodHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onStopPodSandbox
}

func (p *NRITestPlugin) onRemovePodSandboxHook() PodHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onRemovePodSandbox
}

func (p *NRITestPlugin) onCreateContainerHook() ContainerHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onCreateContainer
}

func (p *NRITestPlugin) onStartContainerHook() ContainerHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onStartContainer
}

func (p *NRITestPlugin) onStopContainerHook() ContainerHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onStopContainer
}

func (p *NRITestPlugin) onRemoveContainerHook() ContainerHook {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.onRemoveContainer
}

func (p *NRITestPlugin) recordPodEvent(eventType NRIEventType, pod *nri.PodSandbox) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, NRIEvent{
		Type:         eventType,
		PodSandboxID: pod.GetId(),
		PodName:      pod.GetName(),
		PodNamespace: pod.GetNamespace(),
		PodUID:       pod.GetUid(),
		Timestamp:    time.Now(),
	})
}

func (p *NRITestPlugin) recordContainerEvent(
	eventType NRIEventType,
	pod *nri.PodSandbox,
	container *nri.Container,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, NRIEvent{
		Type:          eventType,
		PodSandboxID:  pod.GetId(),
		PodName:       pod.GetName(),
		PodNamespace:  pod.GetNamespace(),
		PodUID:        pod.GetUid(),
		ContainerID:   container.GetId(),
		ContainerName: container.GetName(),
		Timestamp:     time.Now(),
	})
}
