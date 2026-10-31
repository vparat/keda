// The keda command watches Kubernetes resources and executes configured actions.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vparat/keda/internal/eventcontroller"
)

// Version is the application version supplied at build time.
var Version string


func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		eventcontroller.Start(ctx, cancel)
	}()

	<-ctx.Done()
	slog.Info("Shutting down")
	<-done

	if cause := context.Cause(ctx); cause != nil {
		slog.Debug("stopping", "reason", cause)
	}

}
