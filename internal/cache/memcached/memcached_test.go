package memcached

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

func (f *fakeServer) handler(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		parts := strings.Split(line, " ")
		now := time.Now().Unix()

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
			expiry, _ := strconv.ParseInt(parts[3], 10, 64)
			if expiry > 0 {
				expiry += now
			}
			flags, _ := strconv.ParseUint(parts[2], 10, 32)
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
					fmt.Fprintf(writer, "VALUE %s %d %d\r\n", key, it.flags, len(it.value))
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

		if err := writer.Flush(); err != nil {
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
	t.Cleanup(func() { listener.Close() })

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

	ctx := context.Background()
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

	if _, err := cache.Get(context.Background(), "missing"); err == nil {
		t.Error("Get() expected error for missing key, got nil")
	}
}

func TestMemcachedCacheDelete(t *testing.T) {
	cache, err := New([]string{newFakeServer().serve(t)}, 0)
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	ctx := context.Background()
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

	ctx := context.Background()
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
