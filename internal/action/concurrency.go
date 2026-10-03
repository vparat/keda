package action

import (
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

type Concurrency struct {
	Max     int64
	Timeout time.Duration
}

func ConcurrencyFromEnv() Concurrency {
	settings := Concurrency{Max: 5, Timeout: 5 * time.Minute}
	if value := os.Getenv("CMDRUNNER_SEMAPHORE_MAX"); value != "" {
		n, err := strconv.ParseInt(value, 10, 64)
		if err == nil && n > 0 {
			settings.Max = n
		} else {
			slog.Warn("invalid CMDRUNNER_SEMAPHORE_MAX; using default", "value", value)
		}
	}
	if value := os.Getenv("CMDRUNNER_SEMAPHORE_TIMEOUT"); value != "" {
		duration, err := time.ParseDuration(value)
		if err == nil && duration > 0 {
			settings.Timeout = duration
		} else {
			slog.Warn("invalid CMDRUNNER_SEMAPHORE_TIMEOUT; using default", "value", value)
		}
	}
	return settings
}

// JobKey distinguishes resources and rules without concatenation collisions.
type JobKey struct{ Rule, Name, Namespace string }

// Jobs tracks queued and running work shared by all command runners.
// A zero Jobs value is ready for use.
type Jobs struct{ active sync.Map }

func (j *Jobs) Claim(key JobKey) bool {
	_, exists := j.active.LoadOrStore(key, struct{}{})
	return !exists
}

func (j *Jobs) Release(key JobKey) { j.active.Delete(key) }
