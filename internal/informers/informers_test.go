package informers

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// Every kind accepted during configuration validation must have an informer.
func TestSupportedKindsHaveInformers(t *testing.T) {
	factory := kinformers.NewSharedInformerFactory(fake.NewClientset(), 0)
	for kind := range kindToGVR {
		t.Run(kind, func(t *testing.T) {
			if _, err := createInformer(kind, factory); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreateAndRun(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{Name: "example", Namespace: "default"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages := make(chan Message)
	done := make(chan error, 1)
	go func() { done <- CreateAndRun(ctx, client, "pod", "create", "", "default", "pod-created", messages) }()
	select {
	case got := <-messages:
		want := Message{Key: "pod-created", Name: "example", Namespace: "default"}
		if got != want {
			t.Fatalf("message = %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event received")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("informer did not stop")
	}
}

func TestCreateAndRunInvalidConfiguration(t *testing.T) {
	for _, tt := range []struct{ kind, action string }{{"unknown", "create"}, {"pod", "unknown"}} {
		t.Run(tt.kind+"/"+tt.action, func(t *testing.T) {
			if err := CreateAndRun(context.Background(), fake.NewClientset(), tt.kind, tt.action, "", "", "rule", nil); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}
