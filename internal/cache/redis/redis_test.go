package redis

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/alicebob/miniredis/v2"
)


func TestRedisCacheSetGet(t *testing.T) {
	mr := miniredis.RunT(t)

	cache, err := New([]string{mr.Addr()}, 0)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	ctx := t.Context()
	key := "foo"
	value := []byte("bar")

	if err := cache.Set(ctx, key, value); err != nil {
		t.Fatalf("Set() unexpected error: %v", err)
	}

	got, err := cache.Get(ctx, key)

	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get() = %q, want %q", got, value)
	}

}


func TestRedisCacheGetMissingKey(t *testing.T) {
	mr := miniredis.RunT(t)

	cache, err := New([]string{mr.Addr()}, 0)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	if _, err := cache.Get(t.Context(), "missing"); !errors.Is(err, goredis.Nil) {
		t.Errorf("Get() error = %v, want %v", err, goredis.Nil)
	}

}


func TestRedisCacheDelete(t *testing.T) {
	mr := miniredis.RunT(t)

	cache, err := New([]string{mr.Addr()}, 0)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	ctx := t.Context()
	key := "foo"

	if err := cache.Set(ctx, key, []byte("bar")); err != nil {
		t.Fatalf("Set() unexpected error: %v", err)
	}

	if err := cache.Delete(ctx, key); err != nil {
		t.Fatalf("Delete() unexpected error: %v", err)
	}

	if _, err := cache.Get(ctx, key); err == nil {
		t.Error("Get() expected error after delete, got nil")
	}

}


func TestRedisCacheTTL(t *testing.T) {
	mr := miniredis.RunT(t)

	cache, err := New([]string{mr.Addr()}, 1*time.Second)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	ctx := t.Context()
	key := "foo"

	if err := cache.Set(ctx, key, []byte("bar")); err != nil {
		t.Fatalf("Set() unexpected error: %v", err)
	}

	mr.FastForward(2 * time.Second)

	if _, err := cache.Get(ctx, key); err == nil {
		t.Error("Get() expected error after TTL expiry, got nil")
	}

}


func TestNewNoHosts(t *testing.T) {

	if _, err := New(nil, 0); err == nil {
		t.Error("New() expected error for empty hosts, got nil")
	}

}


// TestRedisCacheClosedClient checks that all operations preserve client failures.
// It receives the test handle and returns no values.
func TestRedisCacheClosedClient(t *testing.T) {
	cache, err := New([]string{"localhost:6379"}, 0)

	if err != nil {
		t.Fatal(err)
	}

	if err := cache.client.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{name: "set", call: func() error {
			return cache.Set(t.Context(), "key", []byte("value"))
		}},
		{name: "get", call: func() error {
			_, err := cache.Get(t.Context(), "key")

			return err
		}},
		{name: "delete", call: func() error {
			return cache.Delete(t.Context(), "key")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()

			if !errors.Is(err, goredis.ErrClosed) || !strings.Contains(err.Error(), "redis "+tt.name) {
				t.Fatalf("operation error = %v; want wrapped closed client error", err)
			}

		})
	}

}
