// Package configwatcher selects sources of event rule configuration.
package configwatcher

import (
	"context"
	"fmt"

	"github.com/vparat/keda/internal/configwatcher/configmap"
	"k8s.io/client-go/kubernetes"
)

// Config selects a source and supplies its Kubernetes client and location.
type Config struct {
	Type      string
	Name      string
	Key       string
	Namespace string
	Client    kubernetes.Interface
}


// Watcher produces configuration snapshots. Watch uses ctx for its lifetime and
// returns a stream whose first value is the initial configuration, followed by
// updates. Each byte slice belongs to the receiver. On success, shutdown cancels
// and joins all source workers; it is safe to call repeatedly. Startup failures
// return an error. Sources close the stream when they stop.
type Watcher interface {
	Watch(ctx context.Context) (<-chan []byte, func(), error)
}


var _ Watcher = configmap.ConfigMap{}

// NewWithConfig returns the selected source, applying defaults to empty fields.
// It returns an error for an unsupported type; ConfigMap requires c.Client.
func NewWithConfig(c Config) (Watcher, error) {

	if c.Type == "" {
		c.Type = "configmap"
	}

	if c.Type != "configmap" {
		return nil, fmt.Errorf("unsupported configuration source %q", c.Type)
	}

	if c.Namespace == "" {
		c.Namespace = "default"
	}

	if c.Name == "" {
		c.Name = "keda-config"
	}

	if c.Key == "" {
		c.Key = "config.yaml"
	}

	if c.Client == nil {
		return nil, fmt.Errorf("configmap source requires a Kubernetes client")
	}

	return configmap.ConfigMap{Client: c.Client, Namespace: c.Namespace, Name: c.Name, Key: c.Key}, nil
}
