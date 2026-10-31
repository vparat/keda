// Package cache constructs interchangeable cache backends.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/vparat/keda/internal/cache/memcached"
	"github.com/vparat/keda/internal/cache/noop"
	"github.com/vparat/keda/internal/cache/redis"
)

var (
	_ Cacher = new(noop.NoopCache)
	_ Cacher = new(redis.RedisCache)
	_ Cacher = new(memcached.MemcachedCache)
)


// Cacher stores, retrieves, and deletes values by key.
type Cacher interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}


// New constructs the selected cache backend with the supplied hosts and TTL.
func New(t string, hosts []string, ttl time.Duration) (Cacher, error) {

	switch t {
	case "redis":
		return redis.New(hosts, ttl)
	case "memcached":
		return memcached.New(hosts, ttl)
	case "noop":
		return noop.New(hosts, ttl)
	default:
		return nil, fmt.Errorf("invalid cache type %q", t)
	}

}
