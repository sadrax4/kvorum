#!/usr/bin/env bash
# Builds kvorum and runs a three-node cluster on localhost.
#
# Node 1: client 127.0.0.1:2379  raft 127.0.0.1:2380  http 127.0.0.1:2381
# Node 2: client 127.0.0.1:12379 raft 127.0.0.1:12380 http 127.0.0.1:12381
# Node 3: client 127.0.0.1:22379 raft 127.0.0.1:22380 http 127.0.0.1:22381
#
# Data and logs live under ./data/nodeN. Ctrl-C stops every node.
set -euo pipefail

cd "$(dirname "$0")/.."
make build >/dev/null

DATA_ROOT="${KVORUM_DATA_ROOT:-./data}"
CLUSTER="1=127.0.0.1:2380,2=127.0.0.1:12380,3=127.0.0.1:22380"
PIDS=()

cleanup() {
  echo
  echo "stopping cluster"
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

start_node() {
  local id=$1 base=$2
  mkdir -p "$DATA_ROOT/node$id"
  ./bin/kvorum \
    --id "$id" \
    --data-dir "$DATA_ROOT/node$id" \
    --listen-client "127.0.0.1:$((base + 2379))" \
    --listen-raft "127.0.0.1:$((base + 2380))" \
    --listen-http "127.0.0.1:$((base + 2381))" \
    --initial-cluster "$CLUSTER" \
    --log-format text \
    >"$DATA_ROOT/node$id.log" 2>&1 &
  PIDS+=($!)
}

start_node 1 0
start_node 2 10000
start_node 3 20000

echo "cluster running; endpoints: 127.0.0.1:2379,127.0.0.1:12379,127.0.0.1:22379"
echo "logs: $DATA_ROOT/node{1,2,3}.log"
echo "try: ./bin/kvorumctl --endpoints 127.0.0.1:2379,127.0.0.1:12379,127.0.0.1:22379 put hello world"
wait
