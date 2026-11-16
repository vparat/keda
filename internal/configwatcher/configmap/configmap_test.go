package configmap

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// receive waits for the next snapshot using t and returns it as text.
func receive(t *testing.T, updates <-chan []byte) string {
	t.Helper()

	select {
	case data, ok := <-updates:

		if !ok {
			t.Fatal("source closed unexpectedly")
		}

		return string(data)
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot timed out")

		return ""
	}
}


// TestWatchLifecycle checks initial data, updates, deletion, recreation, and shutdown.
func TestWatchLifecycle(t *testing.T) {
	cm := &corev1.ConfigMap{Name: "rules", Namespace: "custom", Data: map[string]string{"events": "first"}}
	client := fake.NewClientset(cm)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := ConfigMap{Client: client, Name: "rules", Namespace: "custom", Key: "events"}
	updates, stop, err := source.Watch(ctx)

	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if got := receive(t, updates); got != "first" {
		t.Fatalf("initial = %q", got)
	}
	// The informer emits its initial list after the explicit initial GET.
	if got := receive(t, updates); got != "first" {
		t.Fatalf("list = %q", got)
	}
	cm.Data["events"] = "second"

	if _, err := client.CoreV1().ConfigMaps("custom").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if got := receive(t, updates); got != "second" {
		t.Fatalf("update = %q", got)
	}

	if err := client.CoreV1().ConfigMaps("custom").Delete(ctx, "rules", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	if got := receive(t, updates); got != "second" {
		t.Fatalf("deletion = %q", got)
	}
	cm.Data = map[string]string{"other": "value"}

	if _, err := client.CoreV1().ConfigMaps("custom").Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if got := receive(t, updates); got != "" {
		t.Fatalf("missing key = %q", got)
	}
	cancel()
	stopped := make(chan struct{})
	go func() { stop(); stop(); close(stopped) }()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}

	for range updates {
	}
}


// TestWatchSetupErrors verifies that setup returns errors rather than exiting.
func TestWatchSetupErrors(t *testing.T) {

	for _, tt := range []struct {
		name    string
		source  ConfigMap
		message string
	}{
		{name: "no client", message: "requires a Kubernetes client"},
		{name: "missing map", source: ConfigMap{Client: fake.NewClientset(), Namespace: "default", Name: "missing", Key: "config.yaml"}, message: "get configmap default/missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			updates, stop, err := tt.source.Watch(t.Context())

			if err == nil || !strings.Contains(err.Error(), tt.message) || updates != nil || stop != nil {
				t.Fatalf("Watch = %v, %v", updates, err)
			}
		})
	}
}


// TestWatchBlockedConsumer verifies cancellation joins forwarding without a reader.
func TestWatchBlockedConsumer(t *testing.T) {
	client := fake.NewClientset(&corev1.ConfigMap{Name: "rules", Namespace: "default"})
	ctx, cancel := context.WithCancel(t.Context())
	source := ConfigMap{Client: client, Name: "rules", Namespace: "default", Key: "config.yaml"}
	_, stop, err := source.Watch(ctx)

	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	done := make(chan struct{})
	go func() { stop(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked consumer prevented shutdown")
	}
}


// TestWatchReadFailure verifies API errors retain their cause and source location.
// It takes t and returns no values.
func TestWatchReadFailure(t *testing.T) {
	failure := errors.New("access denied")
	client := fake.NewClientset()
	client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, failure
	})
	source := ConfigMap{Client: client, Namespace: "custom", Name: "rules", Key: "events"}
	updates, stop, err := source.Watch(t.Context())

	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "custom/rules") || updates != nil || stop != nil {
		t.Fatalf("Watch returned updates=%v, error=%v", updates, err)
	}
}
