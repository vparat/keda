package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vparat/keda/internal/eventcontroller"
)

const sleepDuration = 5

var (
	Version string
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()
	go eventcontroller.Start(ctx, cancel)

	<-ctx.Done()
	slog.Info("\nShutting down...")
	time.Sleep(sleepDuration * time.Second)
	if cause := context.Cause(ctx); cause != nil {
		slog.Debug("stopping", "reason", cause)
	}
}
