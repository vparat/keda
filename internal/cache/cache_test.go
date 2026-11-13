package cache

import (
	"reflect"
	"testing"

	"github.com/vparat/keda/internal/cache/memcached"
	"github.com/vparat/keda/internal/cache/noop"
	"github.com/vparat/keda/internal/cache/redis"
)


// TestNew verifies backend selection and no-op fallback for unsupported names.
// It receives the test handle and returns no values.
func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		want    any
	}{
		{name: "redis", backend: "redis", want: redis.RedisCache{}},
		{name: "memcached", backend: "memcached", want: memcached.MemcachedCache{}},
		{name: "noop", backend: "noop", want: noop.NoopCache{}},
		{name: "unsupported", backend: "unknown", want: noop.NoopCache{}},
		{name: "unset", backend: "", want: noop.NoopCache{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := New(tt.backend, []string{"localhost:1234"}, 0)

			if err != nil || reflect.TypeOf(cache) != reflect.TypeOf(tt.want) {
				t.Fatalf("New() = %T, %v; want %T without error", cache, err, tt.want)
			}

		})
	}

}
