package eventcontroller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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

			if got := env("KEDA_TEST_ENV", "default"); got != tt.want {
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
			t.Setenv("CONFIGMAP_NAMESPACE", "custom-ns")
			t.Setenv("CONFIGMAP_NAME", "custom-map")
			t.Setenv("CONFIGMAP_KEY", "custom-key")
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


// TestRunSetupFailures verifies setup errors without starting any rule workers.
// It takes the test handle and returns no values.
func TestRunSetupFailures(t *testing.T) {
	watchErr := errors.New("watch registration failed")

	for _, tt := range []struct {
		name, data, message string
		exists, watched     bool
	}{
		{name: "missing configmap", message: "get configmap default/config"},
		{name: "invalid config", exists: true, data: "bad: field", message: "decode event configuration"},
		{name: "watch failure", exists: true, data: "events: []", watched: true, message: "watch configmap default/config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset()

			if tt.exists {

				if err := client.Tracker().Add(&corev1.ConfigMap{Name: "config", Namespace: "default", Data: map[string]string{"config.yaml": tt.data}}); err != nil {
					t.Fatal(err)
				}

			}

			watched := false
			watch := func(ctx context.Context, got kubernetes.Interface, namespace, name string) (<-chan *corev1.ConfigMap, func(), error) {
				watched = true

				if got != client || namespace != "default" || name != "config" || ctx != t.Context() {
					t.Error("wrong watch arguments")
				}

				return nil, nil, watchErr
			}
			c := newController()
			c.watchConfig = watch
			c.execute = func(context.Context, kubernetes.Interface, []Rule) {
				t.Error("unexpected runner")
			}
			err := c.run(t.Context(), client, "default", "config", "config.yaml")

			if err == nil || !strings.Contains(err.Error(), tt.message) || watched != tt.watched {
				t.Fatalf("error = %v, watched = %v", err, watched)
			}

			if tt.watched && !errors.Is(err, watchErr) {
				t.Fatalf("watch error not wrapped: %v", err)
			}

		})
	}

}


// TestRunUpdates verifies unchanged/invalid updates preserve the runner, valid
// changes stop and join it, and cancellation joins the replacement and watcher.
// It takes the test handle and returns no values.
func TestRunUpdates(t *testing.T) {
	initial := "events: [{disabled: true}]"
	client := fake.NewClientset(&corev1.ConfigMap{Name: "config", Namespace: "default", Data: map[string]string{"config.yaml": initial}})
	updates := make(chan *corev1.ConfigMap)
	transitions := make(chan string, 10)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watch := func(context.Context, kubernetes.Interface, string, string) (<-chan *corev1.ConfigMap, func(), error) {
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
		c.watchConfig = watch
		done <- c.run(ctx, client, "default", "config", "config.yaml")
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
		case updates <- &corev1.ConfigMap{Data: map[string]string{"config.yaml": data}}:
		case <-time.After(5 * time.Second):
			t.Fatal("update not consumed")
		}

	}
	expect("start empty")
	send(initial)
	send("events: [{name: invalid}]")
	send(`events: [{name: next, inbound: {kind: pod, action: create}, outbound: {kind: nil}}]`)
	expect("stop empty")
	expect("start next")
	cancel()
	expect("stop next")
	expect("watch stopped")
}
