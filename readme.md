# keda — Kubernetes Event-Driven Actions

Run shell commands in response to Kubernetes resource changes. keda watches rules stored in a ConfigMap or local file, filters events by resource kind, namespace, and labels, and executes the matching actions inside its container.

Use it for lightweight operational automation: recording resource changes, invoking maintenance scripts, or reacting to configuration updates. Rules reload automatically when the selected configuration source changes.

## Contents

- [Getting started](#getting-started)
- [Use cases](#use-cases)
- [Rule configuration](#rule-configuration)
- [Environment variables](#environment-variables)
- [Execution and caching](#execution-and-caching)
- [Deployment and permissions](#deployment-and-permissions)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Example use cases](#example-use-cases)

## Getting started

### Prerequisites

- A Kubernetes cluster and `kubectl` configured to access it.
- Docker and [kind](https://kind.sigs.k8s.io/) for the local walkthrough below.
- Go matching the version in [go.mod](go.mod) for local builds and tests.

The controller uses in-cluster Kubernetes credentials. Run it as a Pod with a service account; it does not load your local kubeconfig.

### Run locally with kind

The [no-cache example](kubernetes/deployments/kind-no-cache.yaml) includes a service account, permissions, a Deployment, and a ConfigMap containing sample rules.

These example permissions grant `cluster-admin` and are intended for local testing. See [Deployment and permissions](#deployment-and-permissions) before adapting them for a shared cluster.

```sh
# Skip this if your default kind cluster already exists.
kind create cluster

# Build and load a local image.
docker build -t keda:local .
kind load docker-image keda:local

# Replace the example's fixed image tag and apply it.
sed 's|image: keda:.*|image: keda:local|' \
  kubernetes/deployments/kind-no-cache.yaml | kubectl apply -f -

kubectl rollout status deployment/keda -n default
kubectl logs -f deployment/keda -n default
```

In another terminal, trigger a sample ConfigMap rule:

```sh
kubectl create configmap keda-demo --from-literal=message=hello -n default
kubectl delete configmap keda-demo -n default
```

The sample rules log command output for resource creation and deletion. Initial informer synchronization also emits create events for resources that already exist.

## Use cases

### Record deployment changes

Use a log-only action to observe deployment updates without running a command:

```yaml
events:
  - name: observe-deployment-updates
    inbound:
      kind: deployment
      action: update
      namespace: default
      label: app=my-app
    outbound:
      kind: nil
```

Updates include status changes as well as specification changes. A rule does not distinguish rollout completion from other updates.

### Invoke a configuration refresh script

React to changes in selected ConfigMaps:

```yaml
events:
  - name: refresh-application-config
    inbound:
      kind: configmap
      action: update
      namespace: default
      label: automation=refresh
    outbound:
      kind: cmd
      action: /opt/scripts/refresh-config.sh
```

Package or mount the executable script into the keda container. It receives the resource name as its first argument and namespace as its second. The command runs in the controller container, not in the changed resource's Pod.

### Run a fixed maintenance command

End a command with `;` when it should run without appended resource arguments:

```yaml
events:
  - name: inspect-services-after-delete
    inbound:
      kind: service
      action: delete
      namespace: default
    outbound:
      kind: command
      action: "kubectl get services -n default;"
```

The container includes `kubectl` and `jq`. Commands that access Kubernetes use the controller's service account permissions.

## Rule configuration

The ConfigMap deployment examples set `CONFIGWATCHER_SOURCE_OPTIONS` to
`namespace:default,name:keda-config,key:config.yaml`, which reads the `config.yaml`
key of ConfigMap `default/keda-config`. These are also the default options when the environment
variable is unset or empty:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: keda-config
  namespace: default
data:
  config.yaml: |
    events:
      - name: deployment-created
        disabled: false
        inbound:
          kind: deployment
          action: create
          namespace: default
          label: app=my-app
        outbound:
          kind: cmd
          action: echo deployment-created
```

| Field | Purpose |
| --- | --- |
| `name` | Unique name for each distinct enabled rule. |
| `disabled` | Set to `true` to skip the rule; defaults to `false`. |
| `inbound.kind` | Supported resource kind in singular form, such as `pod` or `deployment`. |
| `inbound.action` | `create`, `update`, or `delete`. |
| `inbound.namespace` | Namespace to watch. Omit to watch all namespaces. |
| `inbound.label` | Kubernetes label selector. Omit to match all labels. |
| `outbound.kind` | `cmd` or `command` for shell execution; `nil` for logging only. |
| `outbound.action` | Shell command for command actions. |

Commands run through `sh -c`. Resource names and namespaces are appended as shell-quoted arguments unless the command ends with `;`.

### Supported resources

Supported kinds include workloads (`pod`, `deployment`, `statefulset`, `daemonset`, `job`, `cronjob`), configuration (`configmap`, `secret`), networking (`service`, `ingress`, `networkpolicy`, `endpointslice`), storage, RBAC, and cluster resources.

The complete list and API versions are defined in [`kindToGVR`](internal/informers/informers.go). Custom resource kinds are not dynamically discovered. The cluster must serve the mapped API version for each configured kind.

### Reload behavior

- The selected ConfigMap or file must exist and contain valid, nonempty rules at startup.
- Valid configuration updates replace the active rules automatically.
- Invalid or empty updates are logged; the previous rules remain active. Unchanged contents do not restart rules.
- Deleting the ConfigMap or watched file keeps the active rules until valid configuration is recreated.
- An unexpected source stream closure stops the controller; cancellation joins its active workers.
- Identical inbound/outbound rules collapse to the first entry, regardless of name. Kind/action casing, label ordering, command aliases, and surrounding command whitespace are normalized.
- Different outbound actions for the same inbound event remain separate rules.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `CONFIGWATCHER_TYPE` | `configmap` | Configuration source; `configmap` or `file`. |
| `CONFIGWATCHER_SOURCE_OPTIONS` | `name:keda-config,namespace:default,key:config.yaml` | Comma-separated `key:value` source options. ConfigMap requires `namespace`, `name`, and `key`; file requires `path`, for example `path:/home/config/config.yaml`. |
| `LOG_TYPE` | `json` | `json` or `text`, case insensitive. Unknown values use JSON. |
| `LOG_LEVEL` | `info` | Minimum severity: `debug`, `info`, or `error`, case insensitive. Unknown values use `info`. |
| `CMDRUNNER_SEMAPHORE_MAX` | `5` | Maximum concurrent commands across rules in one process. |
| `CMDRUNNER_SEMAPHORE_TIMEOUT` | `5m` | Maximum wait for a command slot; not a command execution timeout. |
| `CACHE_TYPE` | No-op | `noop`, `redis`, or `memcached`. Empty or unknown values select no-op caching. |
| `CACHE_HOST` | Empty | Comma-separated backend addresses, for example `redis-service:6379`. |
| `CACHE_TTL` | `30m` | Cache entry lifetime as a Go duration. Empty or invalid values log an error and use the default. |

Informer resync runs every 10 minutes. `RESYNC_TIME_DURATION` and `CLUSTER_NAME`
appear in some example manifests but are not read by the current code.

Invalid or nonpositive command concurrency settings log a warning and use their defaults. Logs are written to stderr. Set `CACHE_TTL=30m` explicitly to avoid a parsing error log when using the default duration.

## Execution and caching

### Command lifecycle

Commands share a process-wide concurrency limit enforced by a semaphore. Repeated work for the same rule, resource name, and namespace is skipped while that work is queued or running. The reservation is released after completion, failure, or slot-acquisition timeout.

Running commands are canceled on rule reload or shutdown. Command failures and combined stdout/stderr are logged. There is no configured automatic command retry; write actions to tolerate repeated events.

### Resource-version deduplication

Optional Redis or Memcached caching suppresses events whose resource version already matches the cached value. Cache keys distinguish resource kind, action, name, namespace, and rule. Update callbacks also ignore unchanged resource versions during informer resyncs.

| Backend | Behavior |
| --- | --- |
| No-op | Discards cache writes; suitable for local testing or when persistent deduplication is unnecessary. |
| Redis | Stores resource versions in Redis with the configured TTL. |
| Memcached | Stores resource versions in Memcached with the configured TTL. |

The [Redis deployment example](kubernetes/deployments/kind-redis.yaml) expects a reachable backend at `redis-service:6379`. Provision the backend separately or change `CACHE_HOST`.

Cache failures allow events through. Cache entries are written before action completion, so they do not track successful execution. Cache reads and writes are separate operations; a shared cache does not guarantee exactly-once execution across replicas. Command concurrency limits and active-job tracking are local to each process.

## Deployment and permissions

Use a dedicated service account with permissions appropriate to the configured rules:

- `get`, `list`, and `watch` for the configuration ConfigMap when using the ConfigMap source.
- `list` and `watch` for watched resource types in the required namespaces.
- Any additional permissions required by outbound commands.

Restrict who can modify the rule ConfigMap, file, or mounted Secret: command rules execute shell code with the container's access and service account credentials. Avoid commands that repeatedly modify the resources they watch unless the script handles the resulting events intentionally.

The [Dockerfile](Dockerfile) builds a Linux binary and runs it as a non-root user. The runtime includes Bash, `kubectl`, and `jq`. [Dockerfile-gcloud](Dockerfile-gcloud) also includes the Google Cloud CLI (`gcloud`) and verifies that Bash, gcloud, and kubectl run as that user. Add application-specific executables or scripts to a derived image or mount them into the container. For clusters outside kind, push your image to an accessible registry and replace the sample's image tag and `imagePullPolicy: Never`.

## Development

### Build and test

```sh
make build
./keda version

go test ./...
go test -race ./...
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

`make build` embeds the current Git commit in the binary's version. Tests exercise rule validation, reloads, dispatch, command lifecycle, informer callbacks, cache behavior, context propagation to cache operations, configuration source selection and API failures, and logging. They use fake Kubernetes clients and local cache test servers; no running cluster or external cache service is required. The test environment must permit local socket listeners and filesystem notifications.

### Useful Make targets

| Target | Purpose |
| --- | --- |
| `make build` | Build the local `keda` binary with version metadata. |
| `make test` | Run the Go test suite. |
| `make test-cover` | Generate coverage and open the HTML report. |
| `make lint` | Run `golangci-lint` when installed. |
| `make image` | Tidy/vendor dependencies, build an image tagged with the current commit, and remove `vendor`. |
| `make image-gcloud` | Build the commit-tagged image with the Google Cloud CLI using `Dockerfile-gcloud`. |
| `make kind-load-image` | Load the commit-tagged image into the default kind cluster. |
| `make clean` | Remove the binary, `vendor`, and `cover.out` when present. |
| `make clean-images` | List and remove local `keda` image tags. |

### Project layout

```text
main.go                     Startup, logging, cache configuration
internal/eventcontroller/   Rule validation, reloads, and dispatch
internal/configwatcher/     ConfigMap and file configuration sources
internal/informers/         Kubernetes resource watches and event filtering
internal/action/            Command execution and concurrency controls
internal/cache/             No-op, Redis, and Memcached backends
internal/event/             Event configuration types
internal/util/              Shared option parsing helpers
kubernetes/deployments/     Local deployment examples
```

The event controller wires its Kubernetes configuration, selected configuration source,
resource watchers, and command factory once in a private controller instance.
Startup calls its configuration/reload loop directly; the rule dispatcher owns
informer and action workers. Reloads cancel and join the old workers before
starting replacements. Tests replace dependencies on individual controller
instances to exercise failures and reloads without a live cluster.

When contributing, add tests for behavioral changes, update configuration examples when needed, and run the test suite. Keep `k8s.io/kube-openapi` aligned with the revision required by the Kubernetes modules to avoid incompatible structured-merge-diff types.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `failed to get in-cluster config` | Run the controller inside Kubernetes with service account credentials available. |
| Configuration fails at startup | Check source type and options, ConfigMap location or file path and permissions, and YAML content. |
| Rules do not react to changes | Check enabled rules, singular resource names, namespace/label selectors, and watch permissions. |
| Existing resources trigger commands at startup | Create rules receive the informer's initial resource list. Use idempotent commands. |
| A repeated event is skipped | Check resource-version cache entries and whether the same job is queued or running. |
| Commands cannot acquire slots | Review command duration, the concurrency limit, and slot-acquisition timeout. |
| A command is missing or fails | Verify the executable exists in the image and inspect logged command output. |
| Debug messages are absent | Set `LOG_LEVEL=debug` on the Deployment and restart its Pods. |
| The local image cannot be loaded | Match the Deployment image tag to the image loaded into kind. |

## Example use cases

### Pub/Sub script mount

`kubernetes/deployments/kind-redis-pubsub.yaml` includes the `keda-pub-sub-scripts` ConfigMap and mounts it read-only at `/home/scripts`, with executable permissions. The script is available as `/home/scripts/pub-sub-send.sh` and accepts `<kind> <topic> <name> <namespace>` as positional arguments. All four arguments must be nonempty; KEDA rules supply kind and topic, then KEDA appends name and namespace. The script uses strict Bash error handling and `jq` to encode the JSON payload. The container image must include Bash, `jq`, and the `gcloud` CLI; provide suitable Google Cloud credentials at runtime.

The ConfigMap embeds `internal/scripts/pub-sub-send.sh`; keep its contents in sync when changing the source script.

### Deployment allowlist script mount

The [deployment allowlist example](kubernetes/deployments/kind-redis-allowed-deployments.yaml)
mounts `check-allowed-deployments.sh` from the `keda-allowed-deployment-scripts`
ConfigMap at `/home/scripts`, read-only with executable permissions (`0555`).
Its rule checks deployment-create events in `default`, including deployments
found at startup:

```yaml
events:
  - name: check-allowed-deployments
    inbound:
      kind: deployment
      action: create
      namespace: default
    outbound:
      kind: cmd
      action: '/home/scripts/check-allowed-deployments.sh deployment'
```

The script accepts `<kind> <name> <namespace>`. KEDA appends the resource name
and namespace, so an event for `my-app` in `default` runs:

```sh
/home/scripts/check-allowed-deployments.sh deployment my-app default
```

The static `allowed_deployments` array currently keeps `keda`, `redis`,
`test-allowed-1`, and `test-allowed-2`. Names are matched exactly. The script logs
whether each deployment is allowed and deletes unlisted deployments only in the
supplied namespace. Other resource kinds are skipped, and an empty allowlist
causes an error without deleting anything.

Edit the embedded allowlist before applying the manifest to include every
required deployment. Keep `keda` and `redis` listed when these are the controller
and Redis deployment names. The image needs Bash and kubectl; rebuild and load
the image into kind with the tag specified by the manifest. Redis must be reachable
at `redis-service:6379`.

The ConfigMap embeds [internal/scripts/check-allowed-deployment.sh](internal/scripts/check-allowed-deployment.sh)
under the plural filename `check-allowed-deployments.sh`. Keep the source script
and embedded copy in sync when changing the arguments, allowlist, or behavior.

### Configuration sources

The controller consumes `configwatcher.Watcher`, whose `Watch(ctx)` method returns
configuration bytes, a shutdown callback, and a setup error. The first snapshot
must contain the initial configuration; subsequent snapshots are reloads. Empty
or invalid initial configuration fails startup. Invalid or empty updates keep the
current rules, and unchanged bytes skip reloads. Unexpected stream closure stops
the controller with an error. Cancellation joins the active rules and source workers.

The default `configmap` source creates its own in-cluster Kubernetes client,
reads the initial ConfigMap, and watches its selected `Data` key. Configure its
location with `CONFIGWATCHER_SOURCE_OPTIONS`:

```yaml
env:
  - name: CONFIGWATCHER_TYPE
    value: configmap
  - name: CONFIGWATCHER_SOURCE_OPTIONS
    value: "namespace:default,name:keda-config,key:config.yaml"
```

`CONFIGWATCHER_TYPE` defaults to `configmap`; unsupported values fail startup.
The `namespace`, `name`, and `key` options must all be nonempty for ConfigMap.
The [file source](internal/configwatcher/file/readme.md) uses
`CONFIGWATCHER_TYPE=file` and `CONFIGWATCHER_SOURCE_OPTIONS=path:/home/config/rules.yaml`.
It emits initial contents and watches writes, replacements, and symlink swaps.
The file source requires an explicit `path`; the default options describe a
ConfigMap and cannot configure a file source.
Deleting the ConfigMap retains the last rules until a valid replacement appears.

To add another source, implement `Watcher` and register it in
`internal/configwatcher.NewWithConfig`. Sources own their workers, close their
output stream when stopped, and provide an idempotent shutdown callback that
cancels and joins those workers. No context is stored in source configuration;
each call to `Watch` uses its supplied context.

Source configuration is created with `configwatcher.NewConfig(sourceType)`.
Use `SetOption(key, value)` to supply source-specific settings and
`WithOption(key)` to read them. ConfigMap uses `namespace`, `name`, and `key`;
pass the configuration value to `NewWithConfig` to construct the watcher.
Tests cover missing options, option replacement, isolation between instances, and passing configuration by value.

`util.StringToMap("key1:value1,key2:value2")` parses comma-separated options into
`map[string]string`. It trims surrounding whitespace, splits at the first colon,
keeps empty values, and lets later entries replace earlier duplicate keys.
Entries without a colon or with an empty key are skipped. Empty input returns
an empty, writable map. Values cannot contain commas.

Configuration source options are supplied through `CONFIGWATCHER_SOURCE_OPTIONS`,
for example `namespace:default,name:keda-config,key:config.yaml` for ConfigMap.
Unset or empty options use the default ConfigMap location. Nonempty, wholly
malformed options fail startup; explicit ConfigMap options require all three fields. Tests cover option parsing, required fields, and unsupported source types.

### File watcher with Secret-mounted rules

[kind-file-watcher.yaml](kubernetes/deployments/kind-file-watcher.yaml)
uses `CONFIGWATCHER_TYPE=file` and reads `/home/config/config.yaml` from the
`default/keda-config` Secret. The Secret contains the example event rules in
`stringData.config.yaml`. Its volume mounts the entire `/home/config` directory
read-only so projected Secret updates reach the file watcher through symlink
swaps. The rules file is readable by the container's non-root user.

This example uses no-op caching and does not require Redis. Apply the manifest
with an image that includes the file watcher. Update the Secret's rules and reapply to reload
them after Kubernetes refreshes the mounted volume. No Pod restart is required.
