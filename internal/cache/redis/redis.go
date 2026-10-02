package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client goredis.UniversalClient
	ttl    time.Duration
}

func New(hosts []string, ttl time.Duration) (RedisCache, error) {
	if len(hosts) == 0 {
		return RedisCache{}, fmt.Errorf("no redis hosts provided")
	}

	client := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: hosts,
	})

	return RedisCache{
		client: client,
		ttl:    ttl,
	}, nil
}

func (r RedisCache) Set(ctx context.Context, key string, value []byte) error {
	return r.client.Set(ctx, key, value, r.ttl).Err()
}

func (r RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	return r.client.Get(ctx, key).Bytes()
}

func (r RedisCache) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}
