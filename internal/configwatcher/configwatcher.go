// Package configwatcher selects sources of event rule configuration.
package configwatcher

import (
	"context"
	"fmt"

	"github.com/vparat/keda/internal/configwatcher/configmap"
)

// Config selects a source and supplies its Kubernetes client and location.
type Config struct {
	Type      string
	Name      string
	Key       string
	Namespace string
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
// It returns an error for an unsupported type
func NewWithConfig(c Config) (Watcher, error) {
	switch c.Type {
	case "configmap":
		c, err := configmap.New(c.Namespace, c.Name, c.Key)
		if err != nil {
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unsupported configuration source %q", c.Type)
	}
}
