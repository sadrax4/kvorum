# Architecture

This document is the implementation specification for kvorum. It describes the
packages, the contracts between them, and the invariants each one upholds.
It is written to be read top-down: the consensus core first, then the layers
that give it durability, a network, and a client-facing API.

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

## 1. `raft` — the consensus core

`raft` is a pure, deterministic state machine. It owns no goroutines, timers,
sockets, or files, and depends only on the standard library. The application
drives it through `RawNode`:

| Call | Purpose |
| --- | --- |
| `Tick()` | Advance the logical clock by one tick. Election and heartbeat timeouts are counted in ticks. |
| `Step(m Message)` | Deliver a message from a peer. |
| `Propose(data)` | Append a command to the leader's log. Returns `ErrNotLeader` or `ErrProposalDropped` otherwise. |
| `ProposeConfChange(cc)` | Append a membership change. One change may be in flight at a time. |
| `ReadIndex(ctx)` | Start a linearizable read barrier. |
| `TransferLeader(id)` | Hand leadership to another voter. |
| `HasReady()/Ready()` | Obtain the pending work: state to persist, messages to send, entries to apply. |
| `Advance(rd)` | Acknowledge that a Ready has been fully processed. |
| `ApplyConfChange(cc)` | Called by the application when it applies an `EntryConfChange`; returns the new `ConfState`. |

### Ready processing contract

For each `Ready`, the application must, in order:

1. Persist `Snapshot` if non-empty, then restore the state machine from it.
2. Persist `Entries` and `HardState` (fsync when `MustSync`).
3. Send `Messages`.
4. Apply `CommittedEntries`, passing conf changes to `ApplyConfChange`.
5. Serve reads whose `ReadState.Index` is at or below the applied index.
6. Call `Advance(rd)`.

Steps 3 and 4 may be reordered or run concurrently; steps 1–2 must precede 3.

### Storage

`raft.Storage` is read-only from the library's point of view. `MemoryStorage`
implements it in memory and also exposes `Append`, `SetHardState`,
`ApplySnapshot`, `CreateSnapshot`, and `Compact`. The server replays its WAL
into a `MemoryStorage` at start-up and keeps the two in lock-step afterwards.

### Safety mechanisms implemented

- Randomized election timeouts in `[ElectionTick, 2·ElectionTick)`.
- Pre-Vote: a node first asks whether it *would* win before incrementing its
  term, so a partitioned node cannot force term inflation when it rejoins.
- CheckQuorum: a leader steps down when it loses contact with a quorum; a
  follower in contact with a leader ignores vote requests (unless the vote is
  part of a leadership transfer). This is what makes ReadIndex safe.
- Commit rule from Figure 8 of the paper: the leader only advances the commit
  index through entries of its current term, and appends a no-op entry when
  it is elected to make progress possible.
- Accelerated log backtracking: rejections carry `ConflictTerm`/`ConflictIndex`
  so the leader skips an entire term per round trip.
- Flow control: followers are in `probe`, `replicate`, or `snapshot` state;
  `replicate` streams optimistically with a bounded in-flight window.
- Membership: single-server changes (add voter, add learner, remove node). A
  leader rejects a change while another is uncommitted and until it has
  committed an entry of its own term. The new configuration takes effect when
  the entry is applied.
- ReadIndex: the leader records its commit index, confirms leadership with a
  heartbeat round, and releases the read once the application has applied
  that index. Followers forward `MsgReadIndex` to the leader.
- Leadership transfer: the leader stops accepting proposals, brings the
  target up to date, and sends `MsgTimeoutNow`; the target campaigns
  immediately with `Transfer=true` votes that bypass the CheckQuorum lease.

### Testing

Unit tests exercise each rule in isolation. `sim_test.go` runs a seeded
simulation of a cluster with an adversarial network (partitions, drops,
duplicates, delays, reordering) and crash/restart of nodes, checking after
every step: election safety, log matching, leader completeness, state machine
safety, and monotonic commit indexes; and, once faults stop, liveness.

## 2. `internal/wal` — durability

Segmented append-only log with CRC-protected records, plus a snapshot store.

### API

```go
func Open(dir string, opts Options) (*WAL, Replay, error) // Options: SegmentSize, SyncObserver, Logger
type Replay struct {
    HardState    raft.HardState
    Entries      []raft.Entry      // contiguous, after the latest snapshot marker
    SnapshotMeta raft.SnapshotMeta // latest marker, zero if none
}
func (w *WAL) Save(hs raft.HardState, ents []raft.Entry, sync bool) error
func (w *WAL) SaveSnapshot(meta raft.SnapshotMeta) error
func (w *WAL) ReleaseBefore(index raft.Index) error
func (w *WAL) Sync() error
func (w *WAL) Close() error

type Snapshotter struct{ /* ... */ }
func NewSnapshotter(dir string, logger *slog.Logger) (*Snapshotter, error)
func (s *Snapshotter) Save(snap raft.Snapshot) error
func (s *Snapshotter) Load() (raft.Snapshot, error) // ErrNoSnapshot if none
func (s *Snapshotter) Prune(keep int) error
```

### Format

```
segment file  := header record, record*
record        := length u32 LE | crc32c u32 LE (over type+payload) | type u8 | payload
types         := 0x01 entry (index u64, term u64, type u8, data)
                 0x02 hard state (term u64, vote u64, commit u64)
                 0x03 snapshot marker (index u64, term u64, conf state)
                 0x7F segment header (magic "KVWAL", format version)
```

Segments are named by a monotonically increasing sequence number
(`%016x.wal`). A new segment is cut when the active one exceeds
`Options.SegmentSize`; the new segment begins with the current hard state and
the latest snapshot marker, which makes older segments safe to delete after a
snapshot (`ReleaseBefore`).

### Replay rules

- An entry whose index is at or below the last replayed entry truncates the
  in-memory suffix from that index (leader change overwrote a conflicting
  suffix) before being appended. Entries must otherwise be contiguous.
- A snapshot marker discards replayed entries at or below its index.
- A torn record at the tail of the *last* segment is truncated and logged; a
  bad record anywhere else is corruption and fails `Open`.

Snapshots are stored as `<term>-<index>.snap` files with a CRC and are written
atomically (temp file, fsync, rename, directory fsync).

## 3. `internal/kv` — the state machine

`kv.Store` is an in-memory ordered key-value store with per-key version
metadata, applied deterministically from Raft entries.

```go
type Command struct {
    Op              OpType  // OpPut, OpDelete, OpCompareAndSwap
    Key, Value      []byte
    ExpectedVersion uint64  // CompareAndSwap
    ClientID        uint64  // 0 disables deduplication
    Sequence        uint64
    PrevKV          bool
}
func (c Command) Marshal() []byte
func UnmarshalCommand(b []byte) (Command, error)

type KeyValue struct { Key, Value []byte; Version, CreateRevision, ModRevision uint64 }
type Result struct { KV, PrevKV *KeyValue; Succeeded, Deleted, Duplicate bool; Err error }

func (s *Store) Apply(revision uint64, cmd Command) Result
func (s *Store) Get(key []byte) (KeyValue, bool)
func (s *Store) Scan(start, end []byte, limit int) ([]KeyValue, bool)
func (s *Store) Revision() uint64
func (s *Store) Snapshot() ([]byte, error)
func (s *Store) Restore(data []byte) error
func (s *Store) Watch(start, end []byte, buffer int) *Watcher
```

- `revision` is the Raft log index of the applied entry; it becomes the key's
  `ModRevision` (and `CreateRevision` on creation).
- Exactly-once: the store keeps `(ClientID → last Sequence, Result)`. A command
  whose sequence equals the recorded one returns the recorded result with
  `Duplicate=true` and does not mutate state. The table is bounded (LRU by
  revision) and included in snapshots.
- Ordered iteration uses an in-memory ordered map (skip list or treap) under
  `internal/kv/ordered`, tested against a sorted-slice model.
- Watchers receive events synchronously from `Apply` into a bounded channel;
  a watcher that cannot keep up is cancelled with `ErrWatcherOverflow` and the
  client re-establishes it.

## 4. `internal/transport` — peer networking

gRPC transport implementing `RaftService`. One outbound queue and sender
goroutine per peer, writing to a long-lived client stream (`Pipe`) so that
ordinary messages carry no per-RPC overhead; a write that fails or stalls
beyond `SendTimeout` closes the stream, reports the peer unreachable, and the
next message reopens it. Snapshots are streamed on a dedicated goroutine in
chunks.

```go
type Handler interface {
    Step(ctx context.Context, m raft.Message) error
    ReportUnreachable(id raft.NodeID)
    ReportSnapshot(id raft.NodeID, succeeded bool)
}
type Publisher interface { // optional; records client addresses published by followers
    PublishAttributes(ctx context.Context, id raft.NodeID, clientAddress string) error
}
func New(cfg Config) (*Transport, error)
func (t *Transport) Start() error
func (t *Transport) Stop()
func (t *Transport) Send(msgs []raft.Message)
func (t *Transport) AddPeer(id raft.NodeID, addr string)
func (t *Transport) RemovePeer(id raft.NodeID)
func (t *Transport) Address() string
func (t *Transport) Publish(ctx context.Context, to raft.NodeID, clientAddress string) error
```

Inbound messages are validated (`To` must be the local node) and handed to the
`Handler`. Messages to unknown peers are dropped and counted. Snapshots are
sent through a client-streaming `InstallSnapshot` call in chunks; a follower
publishes its client address to the leader through `Publish`, and the leader
records it in the replicated membership table.

## 5. `internal/node` — the runtime

The node owns the goroutine that drives `RawNode` and implements the Ready
contract above. It also provides:

- `Propose(ctx, cmd) (kv.Result, error)` with waiters keyed by log index and
  term; a waiter is failed with `ErrProposalLost` if a different entry is
  applied at its index.
- `LinearizableRead(ctx)` which issues `ReadIndex`, waits for the matching
  `ReadState`, and then waits for the applied index to reach it.
- Snapshotting when `applied − snapshotIndex > SnapshotThreshold`, followed
  by `Compact` on the in-memory storage and `ReleaseBefore` on the WAL.
- Membership bookkeeping: member addresses travel in `ConfChange.Context` and
  are persisted inside the snapshot envelope alongside the kv payload. A node
  publishes its client address once per term (a leader by proposing an
  attribute entry, a follower by asking the leader through the transport), so
  every node can hand out leader hints with an address.
- Graceful shutdown: a leader hands leadership to its most up-to-date
  follower before stopping.

## 6. `internal/server` — client API

gRPC `KVService`/`AdminService` implementations, plus an HTTP listener with
`/metrics`, `/healthz`, `/readyz`, and `/debug/pprof`. Non-leader nodes reject
writes with `codes.Unavailable` and a `LeaderHint` detail.

## 7. `client` and `cmd/kvorumctl`

A Go client that follows leader hints, retries idempotently using sessions,
and exposes the full API; and a CLI built on it, including a `bench`
subcommand.

## 8. Server flags (`cmd/kvorum`)

```
--id                  uint64   node id (required)
--data-dir            string   data directory (required)
--listen-client       string   KV/Admin gRPC listen address  (default 0.0.0.0:2379)
--listen-raft         string   Raft gRPC listen address      (default 0.0.0.0:2380)
--listen-http         string   metrics/health/pprof address  (default 0.0.0.0:2381)
--advertise-client    string   client address other nodes publish for this node
--advertise-raft      string   raft address peers dial
--initial-cluster     string   id=raftaddr,... for bootstrap (all members)
--join                bool     start empty and wait to be added by the leader
--snapshot-threshold  uint64   entries between snapshots (default 10000)
--tick-interval       duration logical tick length (default 100ms)
--election-tick       int      ticks before an election (default 10)
--heartbeat-tick      int      ticks between heartbeats (default 1)
--log-level           string   debug|info|warn|error (default info)
--log-format          string   json|text (default json)
--version                      print version and exit
```

Every flag can also be set through an environment variable named
`KVORUM_<FLAG>` with dashes replaced by underscores.
