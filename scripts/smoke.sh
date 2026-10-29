#!/usr/bin/env bash
# Smoke-tests a running cluster through kvorumctl: a write must be readable
# from every endpoint and the cluster must report a leader.
set -euo pipefail

cd "$(dirname "$0")/.."
CTL="${KVORUMCTL:-./bin/kvorumctl}"
ENDPOINTS="${KVORUM_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:12379,127.0.0.1:22379}"
KEY="smoke-$$"
VALUE="ok-$(date +%s)"

for attempt in $(seq 1 30); do
  if "$CTL" --endpoints "$ENDPOINTS" --timeout 2s put "$KEY" "$VALUE" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "smoke: cluster did not accept writes" >&2
    exit 1
  fi
  sleep 1
done

IFS=',' read -r -a endpoints <<<"$ENDPOINTS"
for ep in "${endpoints[@]}"; do
  got=$("$CTL" --endpoints "$ep" --timeout 2s get "$KEY")
  if [[ "$got" != *"$VALUE"* ]]; then
    echo "smoke: endpoint $ep returned '$got', want '$VALUE'" >&2
    exit 1
  fi
done

status=$("$CTL" --endpoints "$ENDPOINTS" --timeout 2s status)
if [[ "$status" != *"leader"* ]]; then
  echo "smoke: status does not report a leader: $status" >&2
  exit 1
fi

"$CTL" --endpoints "$ENDPOINTS" --timeout 2s del "$KEY" >/dev/null
echo "smoke: ok"
