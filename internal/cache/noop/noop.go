// Package noop provides a cache backend that discards writes.
package noop

import (
	"context"
	"time"
)


// NoopCache implements the no-op cache backend.
type NoopCache struct{}


// New returns a cache that discards writes; hosts and ttl are ignored.
func New(hosts []string, ttl time.Duration) (NoopCache, error) {
	return NoopCache{}, nil
}


// Set discards value and returns nil.
func (n NoopCache) Set(ctx context.Context, key string, value []byte) error {
	return nil
}


// Get returns empty bytes and no error.
func (n NoopCache) Get(ctx context.Context, key string) ([]byte, error) {
	return []byte(""), nil
}


// Delete does nothing and returns nil.
func (n NoopCache) Delete(ctx context.Context, key string) error {
	return nil
}
