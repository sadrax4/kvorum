# ADR 0002: A segmented write-ahead log with replay-time conflict resolution

**Status:** accepted

## Context

Raft requires that a node never forgets its term, its vote, or an entry it
has acknowledged. The log also needs truncation in two directions: a new
leader may overwrite a conflicting suffix, and snapshots make the prefix
redundant.

A single append-only file is simple but can only be compacted by rewriting.
A database (SQLite, BoltDB) handles all of this but hides the durability
story that this project wants to make explicit.

## Decision

`internal/wal` stores records (entries, hard state, snapshot markers) in
numbered segment files with a CRC-32C per record. Suffix truncation is not
performed on disk: an entry with an index at or below the last written index
is simply appended, and replay resolves the conflict by discarding the
superseded suffix. Prefix compaction deletes whole segments once a snapshot
marker covers their entries; every new segment starts with the current hard
state and the latest marker so that older segments are self-contained.

Only the newest segment may end with a torn record, which is truncated on
open. A record is considered torn when the file ends inside it or when the
file from that point on contains a zero-filled sector, which is what an
interrupted write leaves behind; a checksum failure followed by other data
is corruption and refuses to start, because silently skipping a record could
violate the acknowledgement guarantee. An empty segment left by a crash
between creating the file and writing its header is re-initialised, and a
snapshot file whose marker never reached the log gets the marker written on
the next open.

The in-memory `raft.MemoryStorage` is rebuilt from the log on start-up and
serves all reads; the log is written, never read, while the node runs.

## Consequences

- fsync happens once per Ready when the hard state's term or vote changed or
  entries were appended (`Ready.MustSync`); commit-only updates are not
  synced, which the simulator models by letting crashes lose them.
- Memory holds the entries since the last compaction; the snapshot
  threshold bounds that.
- Replay time is proportional to the data written since the last snapshot,
  not to the size of the store.
