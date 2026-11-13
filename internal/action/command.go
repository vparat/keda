package action

import (
	"context"
	"log/slog"
	"os/exec"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	defaultSemaphoreLimit = 5
	semaphoreTimeout      = 5 * time.Minute
)

// Command runs a shell command subject to a concurrency limit.
type Command struct {
	Logger         slog.Logger
	acquireTimeout time.Duration
	Args           string
	slots          *semaphore.Weighted
}

// NewCommandAction constructs a command runner using c.
func NewCommandAction(c Config) (*Command, error) {
	cmd := &Command{
		Logger:         c.Logger,
		acquireTimeout: c.AcquireTimeout,
		slots:          c.GlobalSemaphore,
	}

	if cmd.acquireTimeout <= 0 {
		cmd.acquireTimeout = semaphoreTimeout
	}

	return cmd, nil
}

// Do executes Args, logging the output or any execution error.
func (c *Command) Do(ctx context.Context) {

	if c.Args == "" {
		c.Logger.Warn("no command args provided")

		return
	}

	timeout := c.acquireTimeout

	if timeout <= 0 {
		timeout = semaphoreTimeout
	}

	acquireCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := c.slots.Acquire(acquireCtx, 1); err != nil {
		c.Logger.Error("failed to acquire semaphore", "error", err)

		return
	}

	defer c.slots.Release(1)

	command := exec.CommandContext(
		ctx,
		"sh",
		"-c",
		c.Args,
	)
	command.WaitDelay = time.Second
	out, err := command.CombinedOutput()
	if err != nil {
		c.Logger.Error(
			"command failed",
			"args",
			c.Args,
			"error",
			err,
			"output",
			string(out),
		)

		return
	}

	c.Logger.Info(
		"command executed",
		"args",
		c.Args,
		"output",
		string(out),
	)
}
