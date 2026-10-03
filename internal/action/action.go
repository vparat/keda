package action

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"
)

type Config struct {
	AcquireTimeout  time.Duration
	GlobalSemaphore *semaphore.Weighted
	LocalSemaphore  int64
	Type            string
	CTX             context.Context
	Logger          slog.Logger
}

type Action interface {
	Do()
}

func New() (Action, error) {
	return NewWithConfig(Config{
		Type: "nil",
		CTX:  context.Background(),
	})
}

func NewWithConfig(c Config) (Action, error) {
	switch strings.ToLower(c.Type) {
	case "command":
		return NewCommandAction(c)
	case "nil":
		return NewNilAction(c)
	default:
		return nil, errors.New("invalid type")
	}

}
