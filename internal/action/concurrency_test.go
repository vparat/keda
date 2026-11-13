package action

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)


func TestConcurrencyFromEnv(t *testing.T) {

	for _, tt := range []struct {
		max, timeout string
		want         Concurrency
	}{
		{max: "", timeout: "", want: Concurrency{Max: 5, Timeout: 5 * time.Minute}},
		{max: "2", timeout: "30s", want: Concurrency{Max: 2, Timeout: 30 * time.Second}},
		{max: "0", timeout: "-1s", want: Concurrency{Max: 5, Timeout: 5 * time.Minute}},
		{max: "bad", timeout: "bad", want: Concurrency{Max: 5, Timeout: 5 * time.Minute}},
	} {
		t.Run(tt.max+"/"+tt.timeout, func(t *testing.T) {
			t.Setenv("CMDRUNNER_SEMAPHORE_MAX", tt.max)
			t.Setenv("CMDRUNNER_SEMAPHORE_TIMEOUT", tt.timeout)

			if got := ConcurrencyFromEnv(); got != tt.want {
				t.Fatalf("settings = %+v, want %+v", got, tt.want)
			}

		})
	}

}


func TestJobs(t *testing.T) {
	var jobs Jobs
	key := JobKey{Rule: "rule", Name: "pod", Namespace: "default"}
	var successes atomic.Int32
	var workers sync.WaitGroup

	for range 50 {
		workers.Go(func() {

			if jobs.Claim(key) {
				successes.Add(1)
			}

		})
	}

	workers.Wait()

	if successes.Load() != 1 {
		t.Errorf("successful Jobs.Claim calls = %d, want 1", successes.Load())
	}

	for _, other := range []JobKey{
		{Rule: "other", Name: "pod", Namespace: "default"},
		{Rule: "rule", Name: "pod", Namespace: "other"},
		{Rule: "rule", Name: "other", Namespace: "default"},
	} {

		if !jobs.Claim(other) {
			t.Fatalf("distinct job rejected: %+v", other)
		}

	}

	jobs.Release(key)

	if !jobs.Claim(key) {
		t.Fatal("completed job cannot run again")
	}

}


func TestCommandSemaphore(t *testing.T) {
	limit := semaphore.NewWeighted(1)

	if err := limit.Acquire(t.Context(), 1); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "ran")
	runner, err := NewCommandAction(Config{
		GlobalSemaphore: limit,
		AcquireTimeout:  10 * time.Millisecond,
		Logger:          *slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if err != nil {
		t.Fatal(err)
	}

	runner.Args = "touch '" + path + "'"
	runner.Do(t.Context())

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("command ran without semaphore slot: %v", err)
	}

	limit.Release(1)
	runner.Do(t.Context())

	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}

	if !limit.TryAcquire(1) {
		t.Fatal("command did not release slot")
	}

	limit.Release(1)
}


func TestCommandCanceledBeforeExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path := filepath.Join(t.TempDir(), "ran")
	limit := semaphore.NewWeighted(1)
	runner, err := NewCommandAction(Config{
		GlobalSemaphore: limit,
		Logger:          *slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if err != nil {
		t.Fatal(err)
	}

	runner.Args = "touch '" + path + "'"
	runner.Do(ctx)

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Do(canceled context) output file error = %v, want %v", err, os.ErrNotExist)
	}

	if !limit.TryAcquire(1) {
		t.Fatal("Do(canceled context) did not release its semaphore slot")
	}

	limit.Release(1)
}


func TestCommandCancellationStopsProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	path := filepath.Join(t.TempDir(), "started")
	limit := semaphore.NewWeighted(1)
	runner, err := NewCommandAction(Config{
		GlobalSemaphore: limit,
		Logger:          *slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if err != nil {
		t.Fatal(err)
	}

	runner.Args = "touch '" + path + "'; exec sleep 30"
	done := make(chan struct{})
	go func() {
		defer close(done)
		runner.Do(ctx)
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	for {
		_, err := os.Stat(path)

		if err == nil {
			break
		}

		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}

		select {
		case <-deadline.C:
			t.Fatal("Do() did not start the command")
		case <-tick.C:
		}

	}

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Do() did not stop the process after context cancellation")
	}

	if !limit.TryAcquire(1) {
		t.Fatal("Do() did not release its semaphore slot after cancellation")
	}

	limit.Release(1)
}
