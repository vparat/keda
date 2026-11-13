package memcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)


type fakeItem struct {
	value  []byte
	flags  uint32
	expiry int64
}


type fakeServer struct {
	mu   sync.Mutex
	data map[string]*fakeItem
}


func newFakeServer() *fakeServer {
	return &fakeServer{data: make(map[string]*fakeItem)}
}


func (f *fakeServer) expired(it *fakeItem, now int64) bool {
	return it.expiry > 0 && it.expiry <= now
}


// handler implements the subset of the text protocol exercised by cache tests.
// It buffers each response so network writes happen outside the store lock.
func (f *fakeServer) handler(conn net.Conn) {
	// Closing a completed test connection requires no further recovery.
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			return
		}

		line = strings.TrimRight(line, "\r\n")
		parts := strings.Split(line, " ")
		now := time.Now().Unix()

		// bytes.Buffer writes cannot fail; only the final network copy can fail.
		var writer bytes.Buffer

		switch parts[0] {
		case "set":
			size, err := strconv.Atoi(parts[4])

			if err != nil {
				return
			}

			body := make([]byte, size+2)

			if _, err := io.ReadFull(reader, body); err != nil {
				return
			}

			expiry, err := strconv.ParseInt(parts[3], 10, 64)

			if err != nil {
				return
			}

			if expiry > 0 {
				expiry += now
			}

			flags, err := strconv.ParseUint(parts[2], 10, 32)

			if err != nil {
				return
			}

			f.mu.Lock()
			f.data[parts[1]] = &fakeItem{value: body[:size], flags: uint32(flags), expiry: expiry}
			f.mu.Unlock()
			writer.WriteString("STORED\r\n")

		case "get", "gets":
			f.mu.Lock()

			for _, key := range parts[1:] {
				it, ok := f.data[key]

				if ok && f.expired(it, now) {
					delete(f.data, key)
					ok = false
				}

				if ok {
					fmt.Fprintf(
						&writer,
						"VALUE %s %d %d\r\n",
						key,
						it.flags,
						len(it.value),
					)
					writer.Write(it.value)
					writer.WriteString("\r\n")
				}

			}

			f.mu.Unlock()
			writer.WriteString("END\r\n")

		case "delete":
			f.mu.Lock()

			if _, ok := f.data[parts[1]]; ok {
				delete(f.data, parts[1])
				writer.WriteString("DELETED\r\n")
			} else {
				writer.WriteString("NOT_FOUND\r\n")
			}

			f.mu.Unlock()

		default:
			writer.WriteString("ERROR\r\n")
		}

		if _, err := io.Copy(conn, &writer); err != nil {
			return
		}

	}

}


func (f *fakeServer) serve(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatalf("fake memcached server failed to listen: %v", err)
	}

	t.Cleanup(func() {
		// Closing the listener stops the accept loop, including on test failure.
		listener.Close()
	})

	go func() {

		for {
			conn, err := listener.Accept()

			if err != nil {
				return
			}

			go f.handler(conn)
		}

	}()

	return listener.Addr().String()
}


func TestMemcachedCacheSetGet(t *testing.T) {
	cache, err := New([]string{newFakeServer().serve(t)}, 0)

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


func TestMemcachedCacheGetMissingKey(t *testing.T) {
	cache, err := New([]string{newFakeServer().serve(t)}, 0)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	if _, err := cache.Get(t.Context(), "missing"); !errors.Is(err, memcache.ErrCacheMiss) {
		t.Errorf("Get() error = %v, want %v", err, memcache.ErrCacheMiss)
	}

}


func TestMemcachedCacheDelete(t *testing.T) {
	cache, err := New([]string{newFakeServer().serve(t)}, 0)

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


func TestMemcachedCacheTTL(t *testing.T) {
	cache, err := New([]string{newFakeServer().serve(t)}, 1*time.Second)

	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	ctx := t.Context()
	key := "foo"

	if err := cache.Set(ctx, key, []byte("bar")); err != nil {
		t.Fatalf("Set() unexpected error: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	if _, err := cache.Get(ctx, key); err == nil {
		t.Error("Get() expected error after TTL expiry, got nil")
	}

}


func TestNewNoHosts(t *testing.T) {

	if _, err := New(nil, 0); err == nil {
		t.Error("New() expected error for empty hosts, got nil")
	}

}


// TestMemcachedCacheInvalidKey checks wrapped client errors for invalid keys.
// It receives the test handle and returns no values.
func TestMemcachedCacheInvalidKey(t *testing.T) {
	cache, err := New([]string{"localhost:11211"}, 0)

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{name: "set", call: func() error {
			return cache.Set(t.Context(), "invalid key", []byte("value"))
		}},
		{name: "get", call: func() error {
			_, err := cache.Get(t.Context(), "invalid key")

			return err
		}},
		{name: "delete", call: func() error {
			return cache.Delete(t.Context(), "invalid key")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()

			if !errors.Is(err, memcache.ErrMalformedKey) || !strings.Contains(err.Error(), "memcached "+tt.name) {
				t.Fatalf("operation error = %v; want wrapped malformed key error", err)
			}

		})
	}

}
