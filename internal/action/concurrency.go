package action

import (
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

// Concurrency specifies the shared command limit and slot acquisition timeout.
type Concurrency struct {
	Max     int64
	Timeout time.Duration
}

// JobKey distinguishes resources and rules without concatenation collisions.
type JobKey struct{ Rule, Name, Namespace string }

// Jobs tracks queued and running work shared by all command runners.
// A zero Jobs value is ready for use.
type Jobs struct{ active sync.Map }

// ConcurrencyFromEnv reads command limits from CMDRUNNER_SEMAPHORE_MAX and
// CMDRUNNER_SEMAPHORE_TIMEOUT, using defaults for missing or invalid values.
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

// Claim reserves key and reports whether it was previously unclaimed.
func (j *Jobs) Claim(key JobKey) bool {
	_, exists := j.active.LoadOrStore(key, struct{}{})

	return !exists
}

// Release removes the reservation for key.
func (j *Jobs) Release(key JobKey) {
	j.active.Delete(key)
}
