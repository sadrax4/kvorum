# ADR 0005: Exactly-once writes through client sessions in the state machine

**Status:** accepted

## Context

A client that times out while waiting for a write cannot know whether the
write was applied. Retrying blindly turns "increment" into "increment twice".
The Raft dissertation (§6.3) recommends that the state machine itself
deduplicate: clients tag each request with a session id and a sequence
number, and the state machine remembers the last sequence it applied per
session together with the response.

## Decision

Every write request may carry a `Session{client_id, sequence}`. `kv.Store`
keeps a bounded table of sessions (least-recently-used eviction by revision)
that is part of the snapshot. A command whose sequence equals the recorded
one returns the recorded result without touching the store; a lower sequence
is rejected with `FailedPrecondition`. The Go client allocates one sequence
per logical request and reuses it across retries, so a retry after a timeout
or a leader change is safe.

The node layer complements this by failing proposals it can no longer track
(`ErrProposalLost`) rather than guessing; the client simply retries.

## Consequences

- Deduplication is deterministic because it lives in the replicated state
  machine, so every node agrees on which retries were duplicates.
- The session table bounds memory; a client idle long enough to be evicted
  falls back to at-least-once semantics, which is the documented trade-off.
