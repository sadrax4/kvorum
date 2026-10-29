# Architecture Decision Records

Each record captures a decision that shaped kvorum, the context it was made
in, and its consequences. Records are numbered in the order they were made
and are never edited after the fact; a superseding decision gets a new record.

| ADR | Title |
| --- | --- |
| [0001](0001-deterministic-raft-core.md) | Raft as a deterministic state machine driven by the application |
| [0002](0002-segmented-write-ahead-log.md) | A segmented write-ahead log with replay-time conflict resolution |
| [0003](0003-single-server-membership-changes.md) | Single-server membership changes instead of joint consensus |
| [0004](0004-linearizable-reads-via-readindex.md) | Linearizable reads through ReadIndex, served by any member |
| [0005](0005-exactly-once-client-sessions.md) | Exactly-once writes through client sessions in the state machine |
| [0006](0006-log-index-as-revision.md) | The Raft log index is the store revision |
