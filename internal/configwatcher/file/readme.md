# File configuration source

The file source implements `configwatcher.Watcher` using `github.com/fsnotify/fsnotify`.
Configure the controller with:

```yaml
env:
  - name: CONFIGWATCHER_TYPE
    value: file
  - name: CONFIGWATCHER_SOURCE_OPTIONS
    value: "path:/home/config/rules.yaml"
```

The file must exist and be readable at startup. The source emits its initial
contents, then changed contents. It watches the parent directory to detect atomic
replacement and symlink swaps, including projected Kubernetes Secret and ConfigMap
volumes. Mount the whole directory rather than using a `subPath` mount when updates
are required. The parent directory itself must remain in place.

Deletion retains the last configuration until the file is recreated. Unchanged
contents do not emit snapshots. The controller rejects empty or invalid updates
and keeps its current rules. Watcher errors and read errors other than a missing
file are logged and close the stream. Cancellation or the idempotent shutdown
callback closes the stream and waits for all watcher workers to finish.

Notifications depend on filesystem support; network filesystems may not provide
the events needed by fsnotify. Watching is limited to the file's parent directory;
changes to symlink targets outside that directory are not monitored.
