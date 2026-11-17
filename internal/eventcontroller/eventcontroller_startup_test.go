package eventcontroller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vparat/keda/internal/util"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// TestParseConfigSelectors checks invalid selectors and valid log-only rules.
// It takes the test handle and returns no values.
func TestParseConfigSelectors(t *testing.T) {
	_, err := parseConfig(`events: [{name: bad, inbound: {kind: pod, action: create, label: "a in ("}, outbound: {kind: nil}}]`)

	if err == nil || !strings.Contains(err.Error(), `event "bad"`) {
		t.Fatalf("selector error = %v", err)
	}

	rules, err := parseConfig(`events: [{name: log, inbound: {kind: pod, action: delete}, outbound: {kind: nil}}]`)

	if err != nil || len(rules) != 1 || rules[0].Outbound.Kind != "nil" {
		t.Fatalf("log-only rules = %+v, error = %v", rules, err)
	}

}


// TestEnv checks nonempty overrides and empty/missing fallback values.
// It takes the test handle and returns no values.
func TestEnv(t *testing.T) {

	for _, tt := range []struct{ name, value, want string }{
		{name: "fallback", value: "", want: "default"},
		{name: "override", value: "custom", want: "custom"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KEDA_TEST_ENV", tt.value)

			if got := util.Env("KEDA_TEST_ENV", "default"); got != tt.want {
				t.Fatalf("env = %q, want %q", got, tt.want)
			}

		})
	}

}


// TestStartWithoutCluster checks that public startup cancels its caller on failure.
// It takes the test handle and returns no values.
func TestStartWithoutCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	Start(ctx, cancel, nil)

	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("startup context error = %v", ctx.Err())
	}

}


// TestStartFailures verifies configuration, client, and controller errors cancel
// the parent and identify the failure in logs. It takes t and returns no values.
func TestStartFailures(t *testing.T) {

	for _, tt := range []struct {
		name, message string
		cfg           *rest.Config
		err           error
	}{
		{name: "configuration", message: "failed to get in-cluster config", err: errors.New("config unavailable")},
		{name: "client", message: "failed to create kubernetes client", cfg: &rest.Config{Host: "https://127.0.0.1", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("invalid CA")}}},
		{name: "controller", message: "controller failed", cfg: &rest.Config{Host: "http://127.0.0.1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CONFIGWATCHER_SOURCE_OPTIONS", "namespace:custom-ns,name:custom-map,key:custom-key")
			var output bytes.Buffer
			oldLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			defer slog.SetDefault(oldLogger)
			ctx, cancel := context.WithCancel(t.Context())
			cancel() // No real API requests are made by the successfully constructed client.
			calls := 0
			c := newController()
			c.configure = func() (*rest.Config, error) {
				return tt.cfg, tt.err
			}
			if tt.name == "controller" {
				c.watchConfig = watcherFunc(func(context.Context) (<-chan []byte, func(), error) {
					return nil, nil, errors.New("get configmap custom-ns/custom-map: unavailable")
				})
			}
			c.start(ctx, func() {
				calls++
			})

			if calls != 1 || !strings.Contains(output.String(), tt.message) {
				t.Fatalf("cancel calls = %d, logs = %q", calls, output.String())
			}

			if tt.name == "controller" && !strings.Contains(output.String(), "custom-ns/custom-map") {
				t.Fatalf("configuration overrides missing from error: %q", output.String())
			}

		})
	}

}


// watcherFunc adapts a test function to the source interface.
type watcherFunc func(context.Context) (<-chan []byte, func(), error)

// Watch calls f with ctx and returns its stream, shutdown callback, and error.
func (f watcherFunc) Watch(ctx context.Context) (<-chan []byte, func(), error) {
	return f(ctx)
}


// TestRunSetupFailures checks source startup, invalid initial data, and early closure.
func TestRunSetupFailures(t *testing.T) {
	watchErr := errors.New("watch registration failed")

	for _, tt := range []struct {
		name, data, message string
		err                 error
		closed              bool
	}{
		{name: "watch failure", err: watchErr, message: "watch config"},
		{name: "invalid config", data: "bad: field", message: "decode event configuration"},
		{name: "empty config", message: "configuration must not be empty"},
		{name: "closed source", closed: true, message: "closed before initial snapshot"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stopped := false
			c := newController()
			c.watchConfig = watcherFunc(func(ctx context.Context) (<-chan []byte, func(), error) {

				if ctx != t.Context() {
					t.Error("wrong context")
				}

				if tt.err != nil {
					return nil, nil, tt.err
				}
				updates := make(chan []byte, 1)

				if !tt.closed {
					updates <- []byte(tt.data)
				}
				close(updates)

				return updates, func() { stopped = true }, nil
			})
			c.execute = func(context.Context, kubernetes.Interface, []Rule) { t.Error("unexpected runner") }
			err := c.run(t.Context(), nil)

			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("error = %v", err)
			}

			if tt.err != nil && !errors.Is(err, tt.err) {
				t.Fatalf("error not wrapped: %v", err)
			}

			if tt.err == nil && !stopped {
				t.Fatal("watcher not stopped")
			}
		})
	}
}


// TestRunUpdates verifies unchanged/invalid updates preserve the runner, valid
// changes stop and join it, and cancellation joins the replacement and watcher.
// It takes the test handle and returns no values.
func TestRunUpdates(t *testing.T) {
	initial := "events: [{disabled: true}]"
	client := fake.NewClientset()
	updates := make(chan []byte, 1)
	updates <- []byte(initial)
	transitions := make(chan string, 10)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watch := func(context.Context) (<-chan []byte, func(), error) {
		return updates, func() {
			transitions <- "watch stopped"
		}, nil
	}
	execute := func(ctx context.Context, _ kubernetes.Interface, rules []Rule) {
		name := "empty"

		if len(rules) > 0 {
			name = rules[0].Name
		}

		transitions <- "start " + name
		<-ctx.Done()
		transitions <- "stop " + name
	}
	done := make(chan error, 1)
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(oldLogger)
	go func() {
		c := newController()
		c.execute = execute
		c.watchConfig = watcherFunc(watch)
		done <- c.run(ctx, client)
	}()
	t.Cleanup(func() {
		cancel()

		select {
		case err := <-done:

			if err != nil {
				t.Error(err)
			}

		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}

	})
	expect := func(want string) {
		t.Helper()

		select {
		case got := <-transitions:

			if got != want {
				t.Fatalf("transition = %q, want %q", got, want)
			}

		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for %q", want)
		}

	}
	send := func(data string) {
		t.Helper()

		select {
		case updates <- []byte(data):
		case <-time.After(5 * time.Second):
			t.Fatal("update not consumed")
		}

	}
	expect("start empty")
	send(initial)
	send("events: [{name: invalid}]")
	send("")
	send("   ")
	send(`events: [{name: next, inbound: {kind: pod, action: create}, outbound: {kind: nil}}]`)
	expect("stop empty")
	expect("start next")
	cancel()
	expect("stop next")
	expect("watch stopped")
}


// TestRunSourceClosure verifies an ended source stops and joins active rules.
func TestRunSourceClosure(t *testing.T) {
	updates := make(chan []byte, 1)
	updates <- []byte("events: []")
	started := make(chan struct{})
	stopped := make(chan struct{})
	sourceStopped := false
	c := newController()
	c.watchConfig = watcherFunc(func(context.Context) (<-chan []byte, func(), error) {
		return updates, func() { sourceStopped = true }, nil
	})
	c.execute = func(ctx context.Context, _ kubernetes.Interface, _ []Rule) {
		close(started)
		<-ctx.Done()
		close(stopped)
	}
	go func() { <-started; close(updates) }()
	err := c.run(t.Context(), nil)

	if err == nil || !strings.Contains(err.Error(), "configuration source closed") {
		t.Fatalf("error = %v", err)
	}

	if !sourceStopped {
		t.Fatal("source not stopped")
	}

	select {
	case <-stopped:
	default:
		t.Fatal("runner not joined")
	}
}


// TestRunCancellationBeforeSnapshot checks cancellation while waiting for initial data.
func TestRunCancellationBeforeSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := false
	c := newController()
	c.watchConfig = watcherFunc(func(context.Context) (<-chan []byte, func(), error) {
		cancel()

		return make(chan []byte), func() { stopped = true }, nil
	})
	c.execute = func(context.Context, kubernetes.Interface, []Rule) { t.Error("unexpected runner") }

	if err := c.run(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if !stopped {
		t.Fatal("source not stopped")
	}
}


// TestRunUnsupportedSource verifies source selection fails before starting rules.
// It takes t and returns no values.
func TestRunUnsupportedSource(t *testing.T) {
	t.Setenv("CONFIGWATCHER_TYPE", "unsupported")
	t.Setenv("CONFIGWATCHER_SOURCE_OPTIONS", "namespace:default,name:rules,key:events")
	c := newController()
	c.execute = func(context.Context, kubernetes.Interface, []Rule) {
		t.Error("unexpected rule execution")
	}
	err := c.run(t.Context(), nil)

	if err == nil || !strings.Contains(err.Error(), "unsupported configuration source") {
		t.Fatalf("source selection error = %v", err)
	}
}


// TestRunSourceOptions checks environment option parsing and constructor errors.
// It takes t and returns no values.
func TestRunSourceOptions(t *testing.T) {
	t.Setenv("CONFIGWATCHER_TYPE", "configmap")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	for _, tt := range []struct {
		name, options, message string
		unset bool
	}{
		{name: "unset uses defaults", unset: true, message: "failed to get in-cluster config"},
		{name: "empty uses defaults", message: "failed to get in-cluster config"},
		{name: "malformed", options: "missing-colon,:value,", message: "invalid CONFIGWATCHER_SOURCE_OPTIONS"},
		{name: "missing name", options: "namespace:custom,key:events", message: "name is required"},
		{name: "missing key", options: "namespace:custom,name:rules", message: "key is required"},
		{name: "missing namespace", options: "name:rules,key:events", message: "namespace is required"},
		{name: "complete", options: " namespace : custom , name : rules , key : events ", message: "failed to get in-cluster config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CONFIGWATCHER_SOURCE_OPTIONS", tt.options)

			if tt.unset {
				if err := os.Unsetenv("CONFIGWATCHER_SOURCE_OPTIONS"); err != nil {
					t.Fatal(err)
				}
			}

			c := newController()
			c.execute = func(context.Context, kubernetes.Interface, []Rule) {
				t.Error("rules started after source setup failure")
			}
			err := c.run(t.Context(), nil)

			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("run error = %v, want %q", err, tt.message)
			}
		})
	}
}
