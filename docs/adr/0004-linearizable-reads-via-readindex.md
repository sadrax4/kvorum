# ADR 0004: Linearizable reads through ReadIndex, served by any member

**Status:** accepted

## Context

A read served from a leader's local state is not linearizable: the leader may
have been deposed without knowing it. The two standard remedies are
ReadIndex (confirm leadership with a heartbeat round before answering) and
lease-based reads (trust that no new leader can exist within a clock-bounded
lease). Leases are faster but depend on bounded clock drift, which a
portfolio project cannot assume of its deployment environment.

## Decision

Reads default to linearizable semantics implemented with ReadIndex (§6.4 of
the dissertation): the leader records its commit index, confirms it still has
a quorum through heartbeat acknowledgements that carry the request context,
and the application serves the read once it has applied up to that index.
Followers forward the request to the leader and wait until their own applied
index reaches the returned read index, so every member can serve
linearizable reads and clients do not need to find the leader for them.

`CheckQuorum` is always enabled, because it is what makes ReadIndex safe: a
leader that lost its quorum steps down within an election timeout, so a stale
leader cannot keep collecting acknowledgements.

Clients that prefer latency over freshness can ask for
`READ_CONSISTENCY_STALE`, which reads the local replica with no coordination.

## Consequences

- Every linearizable read costs one heartbeat round trip on the leader;
  requests arriving between heartbeats share the same round.
- The simulator checks that a confirmed read index is never below the highest
  index any node had applied when the read was requested, which is the
  linearizability condition for reads.
- Lease-based reads could be added later as an opt-in optimisation.
