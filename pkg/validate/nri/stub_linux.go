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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/containerd/nri/pkg/stub"

	"sigs.k8s.io/cri-tools/pkg/framework"
)

// NRITestStub wraps an NRI stub instance for test lifecycle management.
type NRITestStub struct {
	Plugin *NRITestPlugin
	Stub   stub.Stub
	cancel context.CancelFunc
	done   chan struct{}
}

// StartNRITestStub creates and starts an NRI test stub connected to the runtime.
//
// Optional configure callbacks run against the plugin before it connects, so
// tests can install hooks that fire during the registration/Synchronize
// handshake (e.g. OnSynchronize), which cannot be set after this call returns
// because the handshake has already completed by then.
func StartNRITestStub(
	pluginName, pluginIdx string,
	configure ...func(*NRITestPlugin),
) (*NRITestStub, error) {
	socketPath := framework.TestContext.NRISocketPath
	if socketPath == "" {
		return nil, errors.New("NRI socket path not configured")
	}

	plugin := &NRITestPlugin{
		ready: make(chan struct{}),
	}

	for _, c := range configure {
		c(plugin)
	}
	// Use a custom dialer to capture the underlying network connection.
	// If Start() gets stuck (e.g., waiting for Configure that never arrives),
	// we can force-close the connection to free network resources.
	var (
		conn   net.Conn
		connMu sync.Mutex
	)

	dialer := func(path string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return nil, err
		}

		connMu.Lock()
		conn = c
		connMu.Unlock()

		return c, nil
	}

	s, err := stub.New(plugin,
		stub.WithPluginName(pluginName),
		stub.WithPluginIdx(pluginIdx),
		stub.WithSocketPath(socketPath),
		stub.WithDialer(dialer),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NRI stub: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(done)

		errCh <- s.Run(ctx)
	}()

	// Wait for the stub to complete registration and configuration with the runtime.
	// plugin.ready is closed when Synchronize is called (after register/configure handshake).
	select {
	case <-done:
		cancel()

		// s.Run returns nil when the stub's server closes cleanly, so guard the
		// %w verb: wrapping a nil error renders as "%!w(<nil>)".
		if runErr := <-errCh; runErr != nil {
			return nil, fmt.Errorf("NRI stub exited early: %w", runErr)
		}

		return nil, errors.New("NRI stub exited before becoming ready")
	case <-plugin.ready:
		// Registration and configuration complete
	case <-time.After(10 * time.Second):
		cancel()
		// Don't block indefinitely on <-done: stub.Start() may be stuck on an
		// internal channel read (cfgErrC) that doesn't observe context cancellation.
		select {
		case <-done:
			// Goroutine finished; safe to read the error.
			if runErr := <-errCh; runErr != nil {
				return nil, fmt.Errorf("NRI stub did not become ready within 10s: %w", runErr)
			}

			return nil, errors.New("NRI stub exited within 10s without becoming ready")
		case <-time.After(5 * time.Second):
			// Goroutine still running — likely stuck in Start()'s <-cfgErrC read,
			// which does not observe context cancellation. Force-close the
			// underlying connection to break the ttrpc multiplex and unblock
			// the server goroutine. This prevents a leaked connection from
			// remaining open and potentially tainting later serial NRI specs.
			connMu.Lock()
			if conn != nil {
				conn.Close()
			}
			connMu.Unlock()
			// Use a bounded wait for the goroutine to exit. If Start() is truly
			// stuck on <-cfgErrC (which conn.Close() may not unblock), we accept
			// the goroutine leak rather than blocking the test indefinitely.
			go func() {
				select {
				case <-done:
					s.Stop()
				case <-time.After(30 * time.Second):
					// Permanently stuck — attempt Stop() anyway to release resources,
					// then accept the goroutine leak.
					framework.Logf(
						"NRI stub goroutine still running after 30s; calling Stop() and accepting leak",
					)
					s.Stop()
				}
			}()

			return nil, errors.New(
				"NRI stub did not become ready within 10s and failed to stop within 5s",
			)
		}
	}

	return &NRITestStub{
		Plugin: plugin,
		Stub:   s,
		cancel: cancel,
		done:   done,
	}, nil
}

// Stop disconnects the NRI stub from the runtime.
func (ts *NRITestStub) Stop() {
	// Cancel context first to unblock Run()'s select on ctx.Done().
	// Run() will call stub.Stop() internally, which closes connections and
	// waits for in-flight handlers. If a handler is blocked on a test channel,
	// closing connections should unblock it via gRPC cancellation.
	ts.cancel()

	select {
	case <-ts.done:
	case <-time.After(5 * time.Second):
		// Timeout: Run() is stuck (e.g., handler blocked on test channel).
		// Nothing more we can do - proceed with cleanup.
	}
}

// Cleanup stops the stub and resets events.
func (ts *NRITestStub) Cleanup() {
	ts.Stop()
	ts.Plugin.Reset()
}
