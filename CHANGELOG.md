# Changelog

All notable changes to kvorum are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-10-30

First stable release.

### Consensus

- Raft implemented from scratch as a deterministic state machine with no
  third-party dependencies: leader election with Pre-Vote and CheckQuorum,
  pipelined replication with accelerated conflict resolution, the Figure 8
  commit rule, snapshot installation, single-server membership changes with
  learners, ReadIndex with follower forwarding, and leadership transfer.
- Seeded cluster simulator that injects partitions, message loss,
  duplication, reordering, crashes that lose unsynced state, and membership
  churn, checking election safety, leader completeness, log matching, state
  machine safety, monotonicity, read linearizability, and liveness.
- Inbound AppendEntries are validated and an append that would contradict a
  committed entry is refused, so a buggy or hostile peer cannot crash a node.

### Storage

- Segmented write-ahead log with CRC-32C records, replay-time conflict
  resolution, torn-tail recovery that distinguishes interrupted writes from
  corruption, segment release after snapshots, and recovery from a crash
  between a snapshot file and its log marker.
- Atomically written snapshot files with pruning.

### Key-value store

- Ordered skip-list store with per-key version and revision metadata,
  compare-and-swap, range scans, and watch streams.
- Exactly-once writes through client sessions recorded in the replicated
  state machine and included in snapshots.

### Server and client

- gRPC `KVService` and `AdminService` with leader hints, linearizable reads
  from any member, a Go client that follows hints and retries safely, and
  the `kvorumctl` CLI with a load generator.
- Streaming Raft transport (one long-lived stream per peer) with chunked
  snapshot transfer, bounded queues, and unreachable-peer reporting.
- Prometheus metrics, health and readiness endpoints, pprof, structured
  logs, graceful shutdown with leadership handover, and a process that stops
  serving rather than acknowledge writes it cannot persist.

### Operations

- Dynamic membership through the admin API, including adding learners and
  promoting them once caught up.
- Container image, Docker Compose cluster with optional Prometheus, local
  cluster scripts, CI with lint, race-enabled tests, build matrix, protobuf
  drift check, and container build.

[1.0.0]: https://github.com/sadrax4/kvorum/releases/tag/v1.0.0
