package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// write writes data to path using t and returns no values.
func write(t *testing.T, path, data string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}


// expect waits for want on updates using t and returns no values.
func expect(t *testing.T, updates <-chan []byte, want string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {

		select {
		case data, ok := <-updates:

			if !ok {
				t.Fatal("watcher closed before expected snapshot")
			}

			if string(data) == want {
				return
			}

		case <-timer.C:
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}


// TestWatchLifecycle checks writes, replacement, recreation, and shutdown using t.
// It returns no values.
func TestWatchLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	write(t, path, "first")
	source, err := New(path)

	if err != nil {
		t.Fatal(err)
	}
	updates, stop, err := source.Watch(t.Context())

	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	expect(t, updates, "first")
	write(t, path, "second")
	expect(t, updates, "second")
	replacement := path + ".tmp"
	write(t, replacement, "third")

	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	expect(t, updates, "third")

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	write(t, path, "fourth")
	expect(t, updates, "fourth")
	stop()
	stop()

	for range updates {
	}
}


// TestWatchSymlinkSwap checks projected-volume-style updates using t.
// It returns no values.
func TestWatchSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "first"), "initial")
	write(t, filepath.Join(dir, "second"), "updated")

	if err := os.Symlink("first", filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "rules")

	if err := os.Symlink("..data", path); err != nil {
		t.Fatal(err)
	}
	updates, stop, err := (File{Path: path}).Watch(t.Context())

	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	expect(t, updates, "initial")

	if err := os.Symlink("second", filepath.Join(dir, "..next")); err != nil {
		t.Fatal(err)
	}

	if err := os.Rename(filepath.Join(dir, "..next"), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	expect(t, updates, "updated")
}


// TestWatchErrors checks invalid paths and canceled setup using t; it returns no values.
func TestWatchErrors(t *testing.T) {

	if _, err := New(""); err == nil {
		t.Fatal("empty path accepted")
	}

	for _, path := range []string{"", filepath.Join(t.TempDir(), "missing"), t.TempDir(), filepath.Join(t.TempDir(), "missing", "rules")} {
		updates, stop, err := (File{Path: path}).Watch(t.Context())

		if err == nil || updates != nil || stop != nil {
			t.Fatalf("Watch(%q) = %v, %v", path, updates, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, _, err := (File{Path: "rules"}).Watch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Watch error = %v", err)
	}
}


// TestWatchCancellation checks shutdown with an unread stream using t; it returns no values.
func TestWatchCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules")
	write(t, path, "initial")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	updates, stop, err := (File{Path: path}).Watch(ctx)

	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	write(t, path, "updated")
	cancel()
	done := make(chan struct{})
	go func() { stop(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}

	for range updates {
	}
}
