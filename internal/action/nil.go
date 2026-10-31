package action

import (
	"context"
	"log/slog"
)


// Nil logs an event without executing a command.
type Nil struct {
	Logger slog.Logger
}


// NewNilAction constructs a no-op action using the configured logger.
func NewNilAction(c Config) (*Nil, error) {

	return &Nil{
		Logger: c.Logger,
	}, nil
}


// Do logs that the no-op action was triggered.
func (n *Nil) Do(ctx context.Context) {
	n.Logger.Info("nil action triggered")
}
