package memcached

import (
	"context"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type MemcachedCache struct {
	client *memcache.Client
	ttl    time.Duration
}

func New(hosts []string, ttl time.Duration) (MemcachedCache, error) {
	if len(hosts) == 0 {
		return MemcachedCache{}, fmt.Errorf("no memcached hosts provided")
	}

	client := memcache.New(hosts...)

	return MemcachedCache{
		client: client,
		ttl:    ttl,
	}, nil
}

func (m MemcachedCache) Set(ctx context.Context, key string, value []byte) error {
	item := &memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: int32(m.ttl.Seconds()),
	}

	return m.client.Set(item)
}

func (m MemcachedCache) Get(ctx context.Context, key string) ([]byte, error) {
	item, err := m.client.Get(key)
	if err != nil {
		return nil, err
	}

	return item.Value, nil
}

func (m MemcachedCache) Delete(ctx context.Context, key string) error {
	return m.client.Delete(key)
}
