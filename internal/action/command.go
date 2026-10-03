package action

import (
	"context"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	defaultSemaphoreLimit = 5
	semaphoreTimeout      = 5 * time.Minute
	hasSeparator          = false
)

type Command struct {
	Logger slog.Logger
	ctx    context.Context
	Args   string
	m      sync.Mutex
	// jobs           map[string]struct{}
	s              *semaphore.Weighted
	semaphoreLimit int64
}

func NewCommandAction(c Config) (*Command, error) {
	cmd := &Command{
		Logger:         c.Logger,
		ctx:            c.CTX,
		s:              c.GlobalSemaphore,
		semaphoreLimit: c.LocalSemaphore,
	}
	cmd.s = cmd.semaphore()
	return cmd, nil
}

func (c *Command) Do() {
	if c.Args == "" {
		c.Logger.Warn("no command args provided")
		return
	}

	ctx, cancel := context.WithTimeout(c.context(), semaphoreTimeout)
	defer cancel()

	if err := c.semaphore().Acquire(ctx, 1); err != nil {
		c.Logger.Error("failed to acquire semaphore", "error", err)
		return
	}
	defer c.semaphore().Release(1)

	out, err := exec.CommandContext(c.context(), "sh", "-c", c.Args).CombinedOutput()
	if err != nil {
		c.Logger.Error("command failed", "args", c.Args, "error", err, "output", string(out))
		return
	}
	c.Logger.Info("command executed", "args", c.Args, "output", string(out))
}

func (c *Command) semaphore() *semaphore.Weighted {
	c.m.Lock()
	defer c.m.Unlock()
	if c.s == nil {
		if c.semaphoreLimit == 0 {
			c.semaphoreLimit = int64(defaultSemaphoreLimit)
		}
		c.s = semaphore.NewWeighted(c.semaphoreLimit)
	}
	return c.s
}

func (c *Command) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}
