package action

import (
	"context"
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
		{"", "", Concurrency{5, 5 * time.Minute}},
		{"2", "30s", Concurrency{2, 30 * time.Second}},
		{"0", "-1s", Concurrency{5, 5 * time.Minute}},
		{"bad", "bad", Concurrency{5, 5 * time.Minute}},
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
	key := JobKey{"rule", "pod", "default"}
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
		t.Fatalf("claims = %d", successes.Load())
	}
	for _, other := range []JobKey{{"other", "pod", "default"}, {"rule", "pod", "other"}, {"rule", "other", "default"}} {
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
	if err := limit.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ran")
	runner, _ := NewCommandAction(Config{GlobalSemaphore: limit, AcquireTimeout: 10 * time.Millisecond, Logger: *slog.New(slog.NewTextHandler(io.Discard, nil))})
	runner.Args = "touch '" + path + "'"
	runner.Do()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("command ran without semaphore slot: %v", err)
	}
	limit.Release(1)
	runner.Do()
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if !limit.TryAcquire(1) {
		t.Fatal("command did not release slot")
	}
	limit.Release(1)
}
