package configwatcher

import (
	"testing"

	"github.com/vparat/keda/internal/configwatcher/configmap"
	"k8s.io/client-go/kubernetes/fake"
)

// TestNewWithConfig verifies defaults, explicit selection, and errors using t.
func TestNewWithConfig(t *testing.T) {
	client := fake.NewClientset()
	source, err := NewWithConfig(Config{Client: client})

	if err != nil {
		t.Fatal(err)
	}
	cm, ok := source.(configmap.ConfigMap)

	if !ok || cm.Client != client || cm.Name != "keda-config" || cm.Namespace != "default" || cm.Key != "config.yaml" {
		t.Fatalf("source = %#v", source)
	}
	source, err = NewWithConfig(Config{Type: "configmap", Client: client, Name: "custom", Namespace: "ns", Key: "rules"})

	if err != nil {
		t.Fatal(err)
	}
	cm = source.(configmap.ConfigMap)

	if cm.Name != "custom" || cm.Namespace != "ns" || cm.Key != "rules" {
		t.Fatalf("source = %#v", source)
	}

	for _, c := range []Config{{}, {Type: "unknown", Client: client}} {

		if _, err := NewWithConfig(c); err == nil {
			t.Fatalf("expected error for %#v", c)
		}
	}
}
