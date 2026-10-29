# Operations guide

This guide covers running kvorum: starting a cluster, changing its
membership, observing it, and what to expect when things fail.

## Starting a cluster

Every node needs an id, a data directory, and the addresses it listens on
and advertises. The three-node cluster used by `deploy/docker-compose.yml`
looks like this for node 1:

```bash
kvorum \
  --id 1 \
  --data-dir /data \
  --listen-client 0.0.0.0:2379 \
  --listen-raft 0.0.0.0:2380 \
  --listen-http 0.0.0.0:2381 \
  --advertise-client node1:2379 \
  --advertise-raft node1:2380 \
  --initial-cluster 1=node1:2380,2=node2:2380,3=node3:2380
```

The advertised addresses matter: a wildcard listen address such as
`0.0.0.0:2379` is never advertised, so without `--advertise-client` other
nodes cannot include this node in leader hints and clients fall back to
trying their configured endpoints in turn.

`--initial-cluster` lists every founding member with its Raft address. All
founding members must be started with the same list; each one writes the
membership entries into its own log at bootstrap, and the logs are
identical from the first index on. The flag is ignored on later restarts,
because the data directory already holds the membership.

Every flag can also be given as an environment variable: `KVORUM_ID`,
`KVORUM_DATA_DIR`, `KVORUM_INITIAL_CLUSTER`, and so on.

### Ports

| Port | Purpose |
| --- | --- |
| 2379 | Client API (gRPC): `KVService`, `AdminService` |
| 2380 | Raft transport (gRPC), members only |
| 2381 | HTTP: `/metrics`, `/healthz`, `/readyz`, `/status`, `/debug/pprof/` |

### Single node

A single node is a valid cluster (`--initial-cluster 1=127.0.0.1:2380`). It
commits immediately because it is its own quorum. It is useful for
development, and nodes can be added later.

## Changing membership

Membership changes go through the leader and are replicated like any other
write. kvorum applies single-server changes, so add or remove one node at a
time; the leader rejects a second change while the first is in flight.

### Adding a node

1. Start the new node with `--join` and the Raft addresses of the existing
   members. It stays idle until the leader contacts it.

   ```bash
   kvorum --id 4 --data-dir /data --join \
     --initial-cluster 1=node1:2380,2=node2:2380,3=node3:2380 \
     --advertise-raft node4:2380 --advertise-client node4:2379 ...
   ```

2. Add it as a learner. A learner receives the log (via a snapshot if the
   leader's log has been compacted) but does not vote, so a slow or empty
   node cannot stall commitment.

   ```bash
   kvorumctl member add 4 node4:2380 node4:2379 --learner
   ```

3. Once `kvorumctl status` on the new node shows an applied index close to
   the leader's, promote it to a voter.

   ```bash
   kvorumctl member promote 4
   ```

### Removing a node

```bash
kvorumctl member remove 3
```

If the removed node is the leader, it steps down as soon as the change is
applied and the remaining voters elect a new leader. The removed process
notices its own removal and exits; its data directory can be deleted.

### Moving leadership

```bash
kvorumctl transfer-leader 2
```

The leader stops accepting proposals, brings the target up to date, and
asks it to campaign immediately. A transfer that does not complete within
an election timeout is abandoned and the leader resumes. A node that is
stopped with SIGTERM performs the same handover automatically, so rolling
restarts do not wait for election timeouts.

## Reads and writes

- Writes (`put`, `del`, `cas`) must reach the leader. A follower answers
  with `UNAVAILABLE` and a `LeaderHint`; the Go client and the CLI follow it
  transparently.
- Reads default to linearizable semantics and can be served by any member,
  since followers obtain the read index from the leader. `--stale` reads the
  local replica without coordination.
- Every write may carry a client session; the Go client does this
  automatically, which makes retries after timeouts or leader changes
  exactly-once.

## Snapshots and disk usage

A node takes a snapshot every `--snapshot-threshold` applied entries
(default 10 000), writes it to `<data-dir>/snap`, records it in the log, and
releases log segments that the snapshot covers. The two newest snapshots
are kept. `kvorumctl snapshot` forces one.

Disk usage is therefore roughly: the store twice (two snapshots) plus the
log since the last snapshot. The log is held in memory as well, which the
snapshot threshold bounds.

## Observability

- `/metrics` exposes Prometheus metrics under the `kvorum_` prefix: Raft
  term, commit and applied indexes, leader changes, proposals by outcome,
  messages by type, Ready and apply latency, WAL fsync latency, request
  latency by operation, watchers, transport failures, and build info.
- `/readyz` returns 200 only when the node is running and knows a leader;
  `/healthz` returns 503 once the node has stopped because of a storage
  error, so a supervisor restarts the process instead of leaving a member
  that looks alive but cannot write.
- `/status` returns the same information as `kvorumctl status` as JSON.
- Logs are structured (`--log-format json|text`, `--log-level`). Leader
  changes, membership changes, snapshots, and shutdown are logged at info;
  message-level detail at debug.

Useful alerts: `kvorum_raft_is_leader` summing to anything but 1 across the
cluster for more than an election timeout, a growing
`kvorum_raft_commit_index - kvorum_raft_applied_index` gap, rising
`kvorum_transport_send_failures_total`, or `kvorum_wal_sync_duration_seconds`
p99 above a few milliseconds.

## Failure behaviour

| Situation | Behaviour |
| --- | --- |
| A minority of nodes is down or partitioned | Writes and linearizable reads continue; stale reads on the isolated nodes may lag. |
| The leader is isolated | It steps down within an election timeout (CheckQuorum); the majority elects a new leader. Its pending proposals fail with a retryable error. |
| A majority is down | Writes and linearizable reads fail with `UNAVAILABLE` until a quorum returns. Stale reads keep working. |
| A node crashes | On restart it replays its WAL and snapshot, rejoins, and catches up from the leader, through a snapshot if needed. |
| A node's disk is corrupt | The node refuses to start (`wal: corrupt log`). Delete its data directory and re-add it as a fresh learner. |
| A write to the log or a snapshot fails at runtime | The node stops serving (`/healthz` 503, process exits non-zero) rather than acknowledge writes it cannot persist; a failed snapshot is retried with backoff. |
| Membership calls are retried after a timeout | Safe: adding a member that already has the role, promoting a voter, or removing a non-member succeeds without a change. |
| Clock skew | Irrelevant: kvorum uses no wall-clock assumptions for safety. |

## Backups

Stop a node, copy its data directory (`wal/` and `snap/`), and start it
again, or copy the newest file in `snap/` for a point-in-time copy of the
store. Restoring a backup into a *new* cluster requires starting a single
node from that directory with a fresh `--initial-cluster` that contains only
itself; the old membership is replaced by the bootstrap entries only when
the data directory is empty, so for that case delete the `wal/` directory
and keep `snap/`.

## Reference numbers

Measured with `kvorumctl bench` against `scripts/run-local-cluster.sh` on a
4-core container that hosts all three nodes and the client:

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| 1 client, sequential `put` (128 B) | 590 ops/s | 1.5 ms | 5.0 ms |
| 1 client, sequential linearizable `get` | 1 080 ops/s | 0.87 ms | 2.3 ms |
| 16 clients, `put` | 1 320 ops/s | 11.7 ms | 21.5 ms |
| 64 clients, `put` | 1 400 ops/s | 44.7 ms | 66.4 ms |

Expect the single-client latency to track `fsync` latency on the data disk
(two fsyncs per write) and the throughput ceiling to track available CPU,
since every Raft message is one gRPC call.
