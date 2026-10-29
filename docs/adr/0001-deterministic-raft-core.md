# ADR 0001: Raft as a deterministic state machine driven by the application

**Status:** accepted

## Context

A Raft implementation can either own its goroutines, timers, and I/O (the
hashicorp/raft style), or it can be a pure state machine that the application
drives (the etcd/raft style). The first is easier to use; the second is far
easier to test, because every input — ticks, messages, proposals — is an
explicit function call and every output is a value.

kvorum's main goal is to demonstrate a *correct* consensus implementation,
and correctness in distributed systems is established by exploring
adversarial schedules, not by running three processes on a laptop.

## Decision

The `raft` package exposes a `RawNode` with `Tick`, `Step`, `Propose`,
`Ready`, and `Advance`. It owns no goroutines, timers, sockets, or files and
depends only on the standard library. Durability and networking are the
application's responsibility, with one ordering contract: a Ready's entries
and hard state must be stable before its messages are sent, and a snapshot
must be stable before anything after it is applied.

Randomness (election jitter) is injected through `Config.Rand`, and every
iteration over members uses a sorted slice, so that a given sequence of
inputs always produces the same outputs.

## Consequences

- The simulator in `raft/sim_test.go` can run thousands of seeded schedules
  with partitions, drops, duplicates, reordering, crashes, and membership
  changes, and check the Raft safety properties after every step. Any failure
  is reproducible from its seed.
- The runtime in `internal/node` has to implement the Ready loop carefully;
  that logic is concentrated in one goroutine and one file.
- Proposals are not forwarded from followers to the leader inside the
  library; the server returns a leader hint instead. This keeps the library
  small and makes request routing explicit.
