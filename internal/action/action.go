// Package action executes outbound event actions.
package action

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"
)


// Config configures action execution and concurrency limits.
type Config struct {
	AcquireTimeout  time.Duration
	GlobalSemaphore *semaphore.Weighted
	LocalSemaphore  int64
	Type            string
	Logger          slog.Logger
}


// Action performs an outbound operation.
type Action interface {
	Do(ctx context.Context)
}


// New returns a no-op action.
func New() (Action, error) {

	return NewWithConfig(Config{
		Type: "nil",
	})
}


// NewWithConfig constructs the action selected by c.Type.
func NewWithConfig(c Config) (Action, error) {

	switch strings.ToLower(c.Type) {
	case "command":
		return NewCommandAction(c)
	case "nil":
		return NewNilAction(c)
	default:
		return nil, fmt.Errorf("invalid action type %q", c.Type)
	}

}
