#!/bin/bash

set -euo pipefail

if [[ $# -ne 4 || -z $1 || -z $2 || -z $3 || -z $4 ]]; then
  echo "Usage: $0 <kind> <topic> <name> <namespace>" >&2
  exit 1
fi

kind="$1"
topic="$2"
name="$3"
namespace="$4"
start=$(date +%s)-$$
msg=$(jq -cn \
  --arg kind "$kind" \
  --arg name "$name" \
  --arg namespace "$namespace" \
  --arg start "$start" \
  '{kind: $kind, name: $name, namespace: $namespace, start: $start}')

echo "Publishing $kind $name in namespace $namespace to topic $topic"
gcloud pubsub topics publish "$topic" \
  --message="$msg"
