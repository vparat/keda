// The keda command watches Kubernetes resources and executes configured actions.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vparat/keda/internal/cache"
	"github.com/vparat/keda/internal/eventcontroller"
	"github.com/vparat/keda/internal/util"
)

// Version is the application version supplied at build time.
var Version string

const (
	defaultTtl = 30 * time.Minute
)

func main() {
	if version() {
		return
	}
	configureLogger()
	cache, err := buildCache()
	if err != nil {
		slog.Error(err.Error())
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		eventcontroller.Start(ctx, cancel, cache)
	}()

	<-ctx.Done()
	slog.Info("Shutting down")
	<-done

	if cause := context.Cause(ctx); cause != nil {
		slog.Debug("stopping", "reason", cause)
	}

}

// version reads the first command-line argument and prints Version when it is
// "version". It takes no parameters and returns whether main should exit.
func version() bool {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(Version)

		return true
	}

	return false
}

// buildCache reads CACHE_TYPE, CACHE_HOST, and CACHE_TTL from the environment,
// using defaultTtl when the TTL is missing or invalid. It takes no arguments
// and returns the configured cache or an error if cache creation fails.
func buildCache() (cache.Cacher, error) {
	ct := util.Env("CACHE_TYPE", "")
	host := util.Env("CACHE_HOST", "")

	ttl, err := time.ParseDuration(util.Env("CACHE_TTL", ""))
	if err != nil {
		slog.Error(fmt.Sprintf("Cannot parse cache ttl, using default: %d", defaultTtl), "error", err.Error())
		ttl = defaultTtl
	}

	c, err := cache.New(ct, strings.Split(host, ","), ttl)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// configureLogger reads LOG_TYPE and LOG_LEVEL and installs the default logger.
// It takes no arguments and returns no values.
func configureLogger() {
	loggerType := util.Env("LOG_TYPE", "json")
	logLevel := util.Env("LOG_LEVEL", "info")
	level := slog.LevelInfo

	switch strings.ToLower(logLevel) {
	case "error":
		level = slog.LevelError
	case "debug":
		level = slog.LevelDebug
	}

	options := &slog.HandlerOptions{Level: level}
	var handler slog.Handler = slog.NewJSONHandler(os.Stderr, options)

	if strings.EqualFold(loggerType, "text") {
		handler = slog.NewTextHandler(os.Stderr, options)
	}

	slog.SetDefault(slog.New(handler))
	slog.Info("logger configured", "type", loggerType, "configured_level", level.String())
}
