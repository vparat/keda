// Package memcached provides a Memcached cache backend.
package memcached

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)


// MemcachedCache implements the Memcached cache backend.
type MemcachedCache struct {
	client *memcache.Client
	ttl    time.Duration
}


// New constructs a cache using hosts and the expiration TTL.
func New(hosts []string, ttl time.Duration) (MemcachedCache, error) {

	if len(hosts) == 0 {
		return MemcachedCache{}, errors.New("no memcached hosts provided")
	}

	client := memcache.New(hosts...)

	return MemcachedCache{
		client: client,
		ttl:    ttl,
	}, nil
}


// Set stores value under key.
func (m MemcachedCache) Set(ctx context.Context, key string, value []byte) error {
	item := &memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: int32(m.ttl.Seconds()),
	}

	if err := m.client.Set(item); err != nil {
		return fmt.Errorf("memcached set %q: %w", key, err)
	}

	return nil
}


// Get returns the value stored under key, or an error if it is unavailable.
func (m MemcachedCache) Get(ctx context.Context, key string) ([]byte, error) {
	item, err := m.client.Get(key)

	if err != nil {
		return nil, fmt.Errorf("memcached get %q: %w", key, err)
	}

	return item.Value, nil
}


// Delete removes the value stored under key.
func (m MemcachedCache) Delete(ctx context.Context, key string) error {

	if err := m.client.Delete(key); err != nil {
		return fmt.Errorf("memcached delete %q: %w", key, err)
	}

	return nil
}
