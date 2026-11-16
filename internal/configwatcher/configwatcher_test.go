package configwatcher

import (
	filesource "github.com/vparat/keda/internal/configwatcher/file"
	"strings"
	"testing"
)

// TestNewWithConfig checks source selection and constructor errors using t.
// It returns no values.
func TestNewWithConfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	for _, tt := range []struct {
		name       string
		sourceType string
		options    map[string]string
		message    string
	}{
		{name: "empty type", message: `unsupported configuration source ""`},
		{name: "unknown type", sourceType: "unknown", message: `unsupported configuration source "unknown"`},
		{name: "missing name", sourceType: "configmap", message: "name is required"},
		{name: "missing key", sourceType: "configmap", options: map[string]string{"name": "rules"}, message: "key is required"},
		{name: "missing namespace", sourceType: "configmap", options: map[string]string{"name": "rules", "key": "events"}, message: "namespace is required"},
		{name: "cluster configuration unavailable", sourceType: "configmap", options: map[string]string{"name": "rules", "key": "events", "namespace": "custom"}, message: "failed to get in-cluster config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConfig(tt.sourceType)

			for key, value := range tt.options {
				config.SetOption(key, value)
			}

			source, err := NewWithConfig(config)

			if err == nil || !strings.Contains(err.Error(), tt.message) || source != nil {
				t.Fatalf("NewWithConfig = %v, %v; want %q", source, err, tt.message)
			}
		})
	}
}


// TestConfigOptions checks option lookup, replacement, and instance isolation.
// It takes t and returns no values.
func TestConfigOptions(t *testing.T) {
	config := NewConfig("configmap")
	other := NewConfig("file")

	if config.Type != "configmap" || other.Type != "file" || config.WithOption("missing") != "" {
		t.Fatal("incorrect initial configuration")
	}
	config.SetOption("name", "first")

	if config.WithOption("name") != "first" || other.WithOption("name") != "" {
		t.Fatal("options were not stored independently")
	}
	config.SetOption("name", "replacement")

	if config.WithOption("name") != "replacement" {
		t.Fatal("option was not replaced")
	}
	config.SetOption("name", "")

	if config.WithOption("name") != "" {
		t.Fatal("empty option value was not retained")
	}
}


// TestConfigValuePassing verifies options survive passing Config by value.
// It takes t and returns no values.
func TestConfigValuePassing(t *testing.T) {
	config := NewConfig("configmap")
	config.SetOption("namespace", "custom")
	config.SetOption("name", "rules")
	config.SetOption("key", "events")
	check := func(received Config) {
		t.Helper()

		if received.Type != "configmap" {
			t.Fatalf("source type = %q", received.Type)
		}

		for key, want := range map[string]string{"namespace": "custom", "name": "rules", "key": "events"} {

			if got := received.WithOption(key); got != want {
				t.Errorf("option %q = %q, want %q", key, got, want)
			}
		}
	}
	check(config)
}


// TestFileSource checks file source construction and path validation using t.
// It returns no values.
func TestFileSource(t *testing.T) {
	config := NewConfig("file")

	if _, err := NewWithConfig(config); err == nil {
		t.Fatal("file source accepted a missing path")
	}
	config.SetOption("path", "/home/config/rules.yaml")
	source, err := NewWithConfig(config)

	if err != nil {
		t.Fatal(err)
	}
	file, ok := source.(filesource.File)

	if !ok || file.Path != "/home/config/rules.yaml" {
		t.Fatalf("file source = %#v", source)
	}
}
