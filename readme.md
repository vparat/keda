# Kubernetes event driven actions

keda reads named event rules directly from a
Kubernetes ConfigMap and runs outbound actions when matching resources change.

Configuration defaults to ConfigMap `default/keda-config`, key `config.yaml`.
Override these with `CONFIGMAP_NAMESPACE`, `CONFIGMAP_NAME`, and `CONFIGMAP_KEY`.
The ConfigMap must exist at startup. Changes reload rules automatically; invalid
updates are logged and the previous rules remain active. Deleting the ConfigMap
keeps the active rules until a valid ConfigMap is recreated.

```yaml
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

Inbound kinds use singular names. Actions are `create`, `update`, or `delete`.
Omit namespace to watch all namespaces, and omit label to match all labels.
Each distinct enabled rule needs a unique name. Identical inbound/outbound rules
are collapsed to the first entry, regardless of their names. Kind/action casing,
label order, command aliases, and surrounding command whitespace are normalized.
Different actions for the same inbound event remain separate rules.
Create rules also receive existing objects when their informers first synchronize.
Update rules ignore informer resync notifications with unchanged resource versions.

Outbound kinds are `cmd` (or `command`) and `nil` (log only). Commands run through
`sh -c`; the object's name and namespace are appended as quoted arguments unless
the command ends with `;`.

`CMDRUNNER_SEMAPHORE_MAX` controls the shared concurrency limit across all rules
(default `5`). `CMDRUNNER_SEMAPHORE_TIMEOUT` controls how long a job waits for a
slot (default `5m`); it is not a command execution timeout. Invalid or nonpositive
settings log a warning and use the defaults.

Repeated events for the same rule, resource name, and namespace are skipped while
that job is queued or running. The job can run again after completion, failure,
or timeout. Tracking is local to this process; replicas do not share job state.
Running commands are canceled on reload or shutdown.

The service account needs `get`, `list`, and `watch` on the configuration ConfigMap,
and `list` and `watch` on each inbound resource in its configured namespaces.
The example in `kubernetes/deployments/kind.yaml` uses cluster-admin for local testing.

Run checks with `go test ./...`.
