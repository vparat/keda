package noop

import (
	"context"
	"testing"
)

func TestNew(t *testing.T) {
	if _, err := New(nil, 0); err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
}

func TestNoopCacheSet(t *testing.T) {
	cache, err := New(nil, 0)
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	if err := cache.Set(context.Background(), "foo", []byte("bar")); err != nil {
		t.Errorf("Set() unexpected error: %v", err)
	}
}

func TestNoopCacheGet(t *testing.T) {
	cache, err := New(nil, 0)
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	got, err := cache.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Get() = %q, want empty", got)
	}
}

func TestNoopCacheDelete(t *testing.T) {
	cache, err := New(nil, 0)
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	if err := cache.Delete(context.Background(), "foo"); err != nil {
		t.Errorf("Delete() unexpected error: %v", err)
	}
}
