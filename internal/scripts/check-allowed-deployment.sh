#!/bin/bash

set -euo pipefail

if [[ $# -ne 3 || -z $2 || -z $3 ]]; then
    echo "Usage: $0 <kind> <name> <namespace>" >&2
    exit 1
fi

kind="$1"
name="$2"
namespace="$3"

# Add the deployment names that should be kept to this static list.
allowed_deployments=(
    "keda"
    "redis"
    "test-allowed-1"
    "test-allowed-2"
)

if [[ $kind != "deployment" ]]; then
    exit 0
fi

if [[ ${#allowed_deployments[@]} -eq 0 ]]; then
    echo "Configure allowed_deployments before deleting deployments." >&2
    exit 1
fi

for allowed_name in "${allowed_deployments[@]}"; do
    if [[ $name == "$allowed_name" ]]; then
        echo "Deployment $name is in the allowlist"
        exit 0
    fi
done
echo "Deployment $name is not in the allowlist; deleting..."
kubectl delete deployment "$name" --namespace "$namespace" --ignore-not-found
