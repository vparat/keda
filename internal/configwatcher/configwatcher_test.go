package configwatcher

import (
	"strings"
	"testing"
)

// TestNewWithConfig checks source selection and constructor errors using t.
// It returns no values.
func TestNewWithConfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	for _, tt := range []struct {
		name    string
		config  Config
		message string
	}{
		{name: "empty type", message: `unsupported configuration source ""`},
		{name: "unknown type", config: Config{Type: "unknown"}, message: `unsupported configuration source "unknown"`},
		{name: "missing name", config: Config{Type: "configmap"}, message: "name is required"},
		{name: "missing key", config: Config{Type: "configmap", Name: "rules"}, message: "key is required"},
		{name: "missing namespace", config: Config{Type: "configmap", Name: "rules", Key: "events"}, message: "namespace is required"},
		{name: "cluster configuration unavailable", config: Config{Type: "configmap", Name: "rules", Key: "events", Namespace: "custom"}, message: "failed to get in-cluster config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, err := NewWithConfig(tt.config)

			if err == nil || !strings.Contains(err.Error(), tt.message) || source != nil {
				t.Fatalf("NewWithConfig = %v, %v; want %q", source, err, tt.message)
			}
		})
	}
}
