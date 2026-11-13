// Package redis provides a Redis cache backend.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)


// RedisCache implements the Redis cache backend.
type RedisCache struct {
	client goredis.UniversalClient
	ttl    time.Duration
}


// New constructs a cache using hosts and the expiration TTL.
func New(hosts []string, ttl time.Duration) (RedisCache, error) {

	if len(hosts) == 0 {
		return RedisCache{}, errors.New("no redis hosts provided")
	}

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: hosts,
	})

	return RedisCache{
		client: client,
		ttl:    ttl,
	}, nil
}


// Set stores value under key.
func (r RedisCache) Set(ctx context.Context, key string, value []byte) error {

	if err := r.client.Set(
		ctx,
		key,
		value,
		r.ttl,
	).Err(); err != nil {
		return fmt.Errorf("redis set %q: %w", key, err)
	}

	return nil
}


// Get returns the value stored under key, or an error if it is unavailable.
func (r RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := r.client.Get(ctx, key).Bytes()

	if err != nil {
		return nil, fmt.Errorf("redis get %q: %w", key, err)
	}

	return value, nil
}


// Delete removes the value stored under key.
func (r RedisCache) Delete(ctx context.Context, key string) error {

	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis delete %q: %w", key, err)
	}

	return nil
}
