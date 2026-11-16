// Package file provides configuration snapshots from a local file.
package file

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// File watches Path, including replacements and symlink changes in its directory.
type File struct {
	Path string
}

// New validates path and returns a file source or a validation error.
// File access is checked by Watch, rather than by the constructor.
func New(path string) (File, error) {

	if path == "" {
		return File{}, fmt.Errorf("path is required")
	}

	return File{Path: path}, nil
}

// Watch uses ctx to stream the initial contents of Path and subsequent changes.
// It returns snapshots, an idempotent joining shutdown callback, and a setup error.
// Missing files after startup retain the last snapshot until recreated. Other
// read errors and watcher errors are logged and close the stream.
func (f File) Watch(ctx context.Context) (<-chan []byte, func(), error) {

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	if f.Path == "" {
		return nil, nil, fmt.Errorf("path is required")
	}
	path, err := filepath.Abs(f.Path)

	if err != nil {
		return nil, nil, fmt.Errorf("resolve file path: %w", err)
	}
	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		return nil, nil, fmt.Errorf("create file watcher: %w", err)
	}

	// Watch before reading so replacements between setup and the first read
	// produce notifications. Directory events also cover projected secret swaps.
	if err := watcher.Add(filepath.Dir(path)); err != nil {
		_ = watcher.Close()

		return nil, nil, fmt.Errorf("watch directory for %s: %w", path, err)
	}
	initial, err := os.ReadFile(path)

	if err != nil {
		_ = watcher.Close()

		return nil, nil, fmt.Errorf("read initial file %s: %w", path, err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	updates := make(chan []byte, 1)
	current := bytes.Clone(initial)
	updates <- initial
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer cancel()
		defer close(updates)
		defer watcher.Close()
		// The receiver owns emitted slices, so retain a separate comparison copy.

		for {
			select {
			case <-watchCtx.Done():
				return
			case err, ok := <-watcher.Errors:
				if ok {
					slog.Error("file watcher failed", "path", path, "error", err)
				}

				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Name == filepath.Dir(path) && event.Has(fsnotify.Remove|fsnotify.Rename) {
					slog.Error("watched directory removed", "path", path)

					return
				}

				// Re-read on directory changes to catch symlink target swaps as
				// well as ordinary writes. Unchanged contents produce no snapshot.
				data, err := os.ReadFile(path)

				if os.IsNotExist(err) {
					continue
				}

				if err != nil {
					slog.Error("read watched file failed", "path", path, "error", err)

					return
				}

				if bytes.Equal(current, data) {
					continue
				}
				current = bytes.Clone(data)

				select {
				case updates <- data:
				case <-watchCtx.Done():
					return
				}
			}
		}
	}()

	return updates, func() {
		cancel()
		<-done
	}, nil
}
