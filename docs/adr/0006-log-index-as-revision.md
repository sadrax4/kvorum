# ADR 0006: The Raft log index is the store revision

**Status:** accepted

## Context

Clients benefit from a monotonic revision in responses: it orders writes,
lets a watcher know where it is, and gives `mod_revision` a meaning. etcd
maintains a separate MVCC revision counter with full multi-version history.

## Decision

kvorum uses the Raft log index of the applied entry as the revision. Each key
records the revision that created it and the revision that last modified it,
plus a per-key version counter. Response headers carry the serving node's
applied index as the revision.

There is no multi-version history: reads always observe the latest applied
state, and watches start from the moment they are registered.

## Consequences

- Revisions are strictly increasing and identical on every node without any
  extra coordination, and they are directly comparable with Raft metrics.
- Revisions are not dense: leader no-op entries and membership changes
  consume indexes without changing the store.
- Historical reads and watch replay from an old revision would require an
  MVCC layer; the interface leaves room for one.
