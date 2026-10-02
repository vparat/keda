package noop

import (
	"context"
	"time"
)

type NoopCache struct{}

func New(hosts []string, ttl time.Duration) (NoopCache, error) {
	return NoopCache{}, nil
}

func (n NoopCache) Set(ctx context.Context, key string, value []byte) error {
	return nil
}

func (n NoopCache) Get(ctx context.Context, key string) ([]byte, error) {
	return []byte(""), nil
}

func (n NoopCache) Delete(ctx context.Context, key string) error {
	return nil
}
