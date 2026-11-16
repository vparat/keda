// Package configwatcher selects sources of event rule configuration.
package configwatcher

import (
	"context"
	"fmt"

	"github.com/vparat/keda/internal/configwatcher/configmap"
	filesource "github.com/vparat/keda/internal/configwatcher/file"
)

// Config selects a source and supplies its Kubernetes client and location.
type Config struct {
	Type    string
	options map[string]string
}


func NewConfig(t string) Config {
	c := Config{
		Type:    t,
		options: make(map[string]string),
	}
	return c
}


func (c Config) SetOption(k, v string) {
	c.options[k] = v
}


func (c Config) WithOption(k string) string {
	return c.options[k]
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
var _ Watcher = filesource.File{}

// NewWithConfig returns the selected source, applying defaults to empty fields.
// It returns an error for an unsupported type
func NewWithConfig(c Config) (Watcher, error) {
	switch c.Type {
	case "file":
		return filesource.New(c.WithOption("path"))
	case "configmap":
		c, err := configmap.New(
			c.WithOption("namespace"),
			c.WithOption("name"),
			c.WithOption("key"),
		)
		if err != nil {
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unsupported configuration source %q", c.Type)
	}
}
