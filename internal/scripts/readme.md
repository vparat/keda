# Scripts

`check-allowed-deployment.sh <kind> <name> <namespace>` keeps deployment
names listed in its static `allowed_deployments` array and deletes other
deployments in the supplied namespace using `kubectl`. Populate the array, for
example `allowed_deployments=("keda" "my-app")`, before use. An empty list causes
the script to exit with an error. Names are matched exactly; other resource kinds
are skipped.

`pub-sub-send.sh <kind> <topic> <name> <namespace>` publishes a JSON message with
`kind`, `name`, `namespace`, and `start` fields to a Google Cloud Pub/Sub topic.
All four arguments must be nonempty. The script uses strict Bash error handling
and requires `jq` and an authenticated `gcloud` CLI. KEDA rules supply kind and
topic; KEDA appends the resource name and namespace. Keep the embedded copy in
`kubernetes/deployments/kind-redis-pubsub.yaml` in sync with the source script.
