# kvorum

[![CI](https://github.com/sadrax4/kvorum/actions/workflows/ci.yml/badge.svg)](https://github.com/sadrax4/kvorum/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/sadrax4/kvorum)](https://github.com/sadrax4/kvorum/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/sadrax4/kvorum.svg)](https://pkg.go.dev/github.com/sadrax4/kvorum)
[![Go Version](https://img.shields.io/github/go-mod/go-version/sadrax4/kvorum)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**kvorum** is a distributed, strongly consistent key-value store built on a
from-scratch implementation of the Raft consensus algorithm in Go.

The consensus core is a pure, deterministic state machine with zero
third-party dependencies, which makes it possible to verify Raft's safety
properties with a randomized simulation harness that injects network
partitions, message loss, reordering, and crashes across thousands of seeded
runs. Around that core sit a crash-safe segmented write-ahead log,
snapshot-based log compaction, dynamic cluster membership, linearizable reads
via ReadIndex, leadership transfer, exactly-once client semantics, a gRPC API
with a Go client and CLI, Prometheus metrics, and a Docker Compose cluster
for local experimentation.

## Highlights

- **Raft from scratch, verified by simulation.** Leader election with
  Pre-Vote and CheckQuorum, pipelined replication with accelerated conflict
  resolution, the Figure 8 commit rule, snapshots, single-server membership
  changes with learners, ReadIndex, and leadership transfer, in ~1 800 lines
  of standard-library Go. A seeded simulator drives clusters through
  partitions, drops, duplicates, reordering, crashes that lose unsynced
  state, and membership churn, checking the Raft safety properties after
  every step. Every failure is reproducible from its seed.
- **Durability you can read.** A segmented write-ahead log with CRC-32C
  records, replay-time conflict resolution, torn-tail recovery, segment
  release after snapshots, and atomically written snapshot files.
- **Honest consistency.** Writes are linearizable. Reads are linearizable by
  default and can be served by *any* member, because followers obtain the
  read index from the leader. Stale reads are an explicit opt-in. Client
  sessions make retries exactly-once inside the replicated state machine.
- **Operable.** gRPC API with leader hints, a Go client that follows them, a
  CLI with a load generator, Prometheus metrics, health and readiness
  endpoints, pprof, structured logs, graceful shutdown with leadership
  handover, and a three-node Docker Compose cluster.
- **Tested the way it should be.** Unit tests for every rule, a deterministic
  simulator, property tests against reference models, fuzz tests for every
  decoder, end-to-end tests over real gRPC, all under the race detector in
  CI.

## Architecture

```
                       ┌──────────────────────────────┐
   kvorumctl / client  │        cmd/kvorum (main)      │
          │            │  flags, wiring, lifecycle     │
          ▼            └──────────────┬───────────────┘
   ┌──────────────┐                   │
   │  gRPC  (KV,  │◄──────────────────┤
   │  Admin, HTTP)│   internal/server │
   └──────┬───────┘                   │
          │ proposals / reads         │
          ▼                           ▼
   ┌─────────────────────────────────────────────────┐
   │ internal/node — the Ready loop                   │
   │  • drives raft.RawNode                           │
   │  • persists HardState + entries (internal/wal)   │
   │  • applies committed entries (internal/kv)       │
   │  • sends messages (internal/transport)           │
   │  • snapshots, membership, proposal waiters       │
   └───────┬───────────────┬───────────────┬──────────┘
           │               │               │
     ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼────────┐
     │   raft    │   │ internal/ │   │  internal/   │
     │ (RawNode) │   │    wal    │   │  transport   │
     │ stdlib    │   │ segments, │   │  gRPC peers, │
     │ only      │   │ snapshots │   │  streaming   │
     └───────────┘   └───────────┘   └──────────────┘
```

The `raft` package owns no goroutines, timers, sockets, or files. The
application calls `Tick`, `Step`, and `Propose`, receives a `Ready` that says
what to persist, send, and apply, and acknowledges it with `Advance`. That
single design decision ([ADR 0001](docs/adr/0001-deterministic-raft-core.md))
is what makes exhaustive simulation possible.

A write travels: client → leader's gRPC server → `Propose` → appended to
the WAL and replicated → committed by a quorum → applied to the in-memory
store on every node → the proposal's waiter is resolved with the result.

See [docs/architecture.md](docs/architecture.md) for the package contracts,
[docs/raft-internals.md](docs/raft-internals.md) for the subtle parts of the
consensus implementation, and [docs/adr/](docs/adr/) for the decisions and
their trade-offs.

## Install

Pre-built binaries for Linux and macOS (amd64 and arm64) are attached to
every [release](https://github.com/sadrax4/kvorum/releases), with a
`SHA256SUMS` file. The container image is published to GitHub Container
Registry:

```bash
docker pull ghcr.io/sadrax4/kvorum:latest
```

With Go 1.26 or newer installed, the binaries can also be built from source:

```bash
go install github.com/sadrax4/kvorum/cmd/kvorum@latest
go install github.com/sadrax4/kvorum/cmd/kvorumctl@latest
```

## Quick start

### Three nodes with Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up --build -d
docker compose -f deploy/docker-compose.yml exec node1 kvorumctl put greeting hello
docker compose -f deploy/docker-compose.yml exec node3 kvorumctl get greeting
docker compose -f deploy/docker-compose.yml exec node2 kvorumctl status
```

Add `--profile monitoring` to the `up` command to get Prometheus on
<http://localhost:9090> scraping all three nodes.

### Three local processes

```bash
make build
./scripts/run-local-cluster.sh        # nodes on 2379, 12379, 22379
```

In another terminal:

```bash
export KVORUM_ENDPOINTS=127.0.0.1:2379,127.0.0.1:12379,127.0.0.1:22379
./bin/kvorumctl put app/config/timeout 30s
./bin/kvorumctl get app/config/timeout
./bin/kvorumctl scan --prefix app/
./bin/kvorumctl cas app/config/timeout 1 45s     # succeeds only if version == 1
./bin/kvorumctl watch app/ --prefix              # streams changes
./bin/kvorumctl status
./bin/kvorumctl members
./bin/kvorumctl transfer-leader 2
./bin/kvorumctl bench --op mixed --clients 32 --total 50000
./scripts/smoke.sh
```

Writes sent to a follower are redirected to the leader automatically; reads
can be answered by whichever node you talk to.

### Go client

```go
import "github.com/sadrax4/kvorum/client"

c, err := client.New(client.Config{Endpoints: []string{"127.0.0.1:2379", "127.0.0.1:12379"}})
if err != nil {
    return err
}
defer c.Close()

if _, err := c.Put(ctx, []byte("users/42"), []byte(`{"name":"ada"}`)); err != nil {
    return err
}
kv, err := c.Get(ctx, []byte("users/42"))                      // linearizable
_, err = c.Get(ctx, []byte("users/42"), client.WithStale())    // local replica
res, err := c.CompareAndSwap(ctx, []byte("users/42"), kv.Version, []byte(`{"name":"ada","admin":true}`))
events, err := c.Watch(ctx, []byte("users/"), client.WithPrefix())
for ev := range events {
    fmt.Println(ev.Type, string(ev.KV.Key), ev.KV.ModRevision)
}
```

Every write carries a session (client id + sequence number). A retry after a
timeout or a leader change reuses the sequence, and the replicated state
machine returns the recorded result instead of applying the write twice.

## Guarantees

| Operation | Guarantee |
| --- | --- |
| `Put`, `Delete`, `CompareAndSwap` | Linearizable. Acknowledged only after a quorum has durably replicated the entry and the serving node has applied it. |
| `Get`, `Scan` (default) | Linearizable from any member: the read reflects every write acknowledged before it started. |
| `Get`, `Scan` with `STALE` | Serializable snapshot of the local replica; may lag, never goes backwards. |
| `Watch` | Every change after registration, in revision order, until the watcher falls behind (then it is told so). |
| Retries with a session | Exactly-once: duplicates return the original result. |
| Durability | Term, vote, and entries are fsynced before any message that depends on them is sent. |

What it does not do (yet): TLS and authentication, multi-version history and
watch replay from a past revision, joint consensus, lease-based reads, and
asynchronous apply. Each is a deliberate scope decision recorded in the ADRs
and listed in the roadmap.

## How it is tested

| Layer | What runs |
| --- | --- |
| `raft` unit tests | One test per rule: elections, Pre-Vote vs. no Pre-Vote, CheckQuorum, leases, conflict backtracking, Figure 8, snapshots, membership (add, promote, remove leader), ReadIndex on leader and follower, transfer, restart, bootstrap. |
| `raft` simulator | Three scenarios (3 nodes; 5 nodes with snapshots; 3 + 3 spare nodes with membership churn) × N seeds. Faults: drops, duplicates, delays, reordering, symmetric partitions, one-way link failures, crashes with loss of unsynced commit updates, restarts, transfers. Invariants: election safety, leader completeness, log matching, state machine safety, monotonic term and commit, read linearizability, liveness after healing. |
| `wal` | Replay with overwrites and markers, segment cut and release, torn tail, corruption detection, fuzzed record decoder, snapshot atomicity. |
| `kv` | Operation semantics, sessions, snapshot round trips, watchers, a 20 000-operation property test of the skip list against a sorted-slice model, fuzzed command decoder. |
| `transport` | Real sockets: delivery, multi-megabyte snapshot streaming, rejection of misaddressed messages, unreachable peers, shutdown with full queues, codec round trips. |
| `test/integration` | Real nodes over loopback gRPC: replication, linearizable reads from followers, leader hints, exactly-once retries, CAS, leader failover and restart, learner catch-up through a snapshot, promotion, removal of the leader, watches, leadership transfer, graceful handover. |

A 40-seed simulator run on a laptop exercises, across the three scenarios,
roughly 1 500 elections, 1 700 crashes, 1 400 partitions, 1 300 membership
changes, and 18 000 linearizable reads without a violation; 300 seeds per
scenario pass in about 20 seconds.

Statement coverage is 78% from the per-package unit tests alone and 90%
once the integration tests are counted (`go test -coverpkg=./... ./...`).
The integration suite also includes a small Jepsen-style check: eight
clients increment one counter with compare-and-swap while the leader is
crashed and restarted repeatedly, and the final value must equal the number
of acknowledged increments.

```bash
make test                          # everything, with -race
make test-sim SIM_SEEDS=500        # more simulator seeds
go test ./raft -run TestSimulation -sim.seed=61 -v   # replay one seed
```

## Performance

Numbers from a 4-core container running all three nodes, the client, and
the benchmark on the same machine, so they reflect the implementation rather
than the hardware. Each committed batch costs one fsync on a quorum.

End to end, through `kvorumctl bench` against the three-process cluster:

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| 1 client, sequential `put` (128 B) | 720 ops/s | 1.3 ms | 2.7 ms |
| 1 client, sequential linearizable `get` | 2 030 ops/s | 0.45 ms | 1.4 ms |
| 64 clients, `put` | 2 220 ops/s | 28 ms | 42 ms |

A write costs two fsyncs (leader and one follower) plus two network round
trips, which is where the single-client latency goes. Under concurrency the
shared box saturates: a CPU profile of the leader shows the time split
between fsync, the kernel network path, and gRPC/HTTP2 framing, with the
Raft code itself far down the list.

The transport originally issued one unary RPC per Raft message. Profiling
showed the framing overhead and the per-message round trip dominating, so
each peer now writes to a single long-lived stream instead. On the same box
that change raised 64-client write throughput from 1 400 to 2 220 ops/s and
halved linearizable read latency (0.87 ms to 0.45 ms); the before and after
numbers are both recorded in [docs/operations.md](docs/operations.md).

Micro-benchmarks (`make bench`):

| Benchmark | Result |
| --- | --- |
| WAL append, no fsync | 0.8 µs/op, 4 allocs |
| WAL append + fsync | ~177 µs/op (bounded by the disk) |
| Store put / get | 2.0 µs / 1.5 µs |
| Skip list put / get | 0.9 µs / 0.8 µs |
| Command encode / decode | 93 ns / 116 ns |
| Snapshot of 100k keys | 3.2 ms |

## Project layout

```
raft/               consensus core (stdlib only) and the simulator
internal/wal/       write-ahead log and snapshot store
internal/kv/        key-value state machine; ordered/ holds the skip list
internal/transport/ gRPC transport between members
internal/node/      the Ready loop: persistence, apply, reads, snapshots, membership
internal/server/    gRPC KV/Admin services and HTTP endpoints
internal/telemetry/ Prometheus metrics, logging, build info
client/             Go client with leader following and sessions
cmd/kvorum/         server binary
cmd/kvorumctl/      CLI and load generator
api/proto/          protobuf definitions (buf), generated code in api/*pb
test/integration/   end-to-end tests
deploy/             Docker Compose cluster and Prometheus config
docs/               architecture, Raft internals, operations, ADRs
```

## Development

```bash
make build        # binaries into ./bin
make test         # unit + simulation + integration, -race
make lint         # golangci-lint v2 + buf lint
make proto        # regenerate from api/proto
make cover        # coverage profile
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

## Roadmap

Things that are deliberately out of scope today and would come next:

- Asynchronous apply and snapshotting off the Ready loop.
- Batching acknowledgements and heartbeats across followers.
- TLS and client authentication.
- Multi-version history with watch replay from an old revision.
- Lease-based reads as an opt-in, clock-dependent fast path.
- Joint consensus for multi-node membership changes.

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable
  Consensus Algorithm (Extended Version)*, 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice*, PhD dissertation,
  Stanford, 2014 — membership changes (§4), client interaction (§6).
- The etcd Raft library, whose `Ready`/`Advance` interface this project's
  core is modelled on.

## License

MIT — see [LICENSE](LICENSE).
