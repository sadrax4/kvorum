// Package kv implements the deterministic key-value state machine that kvorum
// replicates through Raft.
//
// A [Store] holds an ordered in-memory map from keys to values with per-key
// version metadata ([KeyValue]). Every mutation enters through [Store.Apply]
// as a [Command] tagged with the Raft log index of the entry that carries it,
// called the revision. Two stores fed the same sequence of (revision, Command)
// pairs reach identical state and produce identical snapshots, which is what
// lets every replica apply the log independently.
//
// # Revisions and versions
//
// A key's ModRevision is the revision of the last command that wrote it and
// CreateRevision the revision that created it. Version counts writes since
// creation: the first write stores version 1, each overwrite increments it,
// and deleting the key resets it so that a later write starts again at 1.
// [Store.Revision] reports the revision of the last command handed to Apply,
// whether or not it changed anything.
//
// # Exactly-once semantics
//
// Clients that retry writes tag them with a ClientID and a per-client
// Sequence. The store remembers, for each client, the last sequence it
// applied together with its [Result]. A retry carrying that sequence returns
// the recorded result with Duplicate set and leaves the state untouched; a
// sequence older than the recorded one fails with [ErrStaleSequence]. The
// table holds at most [Options.MaxSessions] clients and evicts the one whose
// most recent command has the lowest revision. It is part of snapshots so
// that deduplication survives restarts and leadership changes.
//
// # Key ranges
//
// [Store.Scan] takes a half-open range [start, end) in which an empty end
// extends to the largest key. [Store.Watch] uses the same form with two
// differences dictated by the watch API: an empty end selects the single key
// equal to start, and an end consisting of the single byte 0x00 extends to
// the largest key, mirroring the etcd convention.
//
// # Watchers
//
// Events are produced inside Apply, after the mutation and in revision order,
// and delivered to each watcher's bounded channel with a non-blocking send. A
// watcher whose channel is full is cancelled with [ErrWatcherOverflow], so a
// slow consumer can never stall the state machine; the consumer re-reads the
// keys it cares about and watches again. Restoring a snapshot cancels
// watchers the same way, because the events between the old and the new state
// are not available to replay.
//
// # Concurrency
//
// Apply, Restore, Watch, and [Watcher.Cancel] take the store's write lock;
// Get, Scan, Snapshot, and the counters take its read lock. Every byte slice
// that leaves the store is a copy and every slice passed in is copied before
// it is retained, so callers can neither observe nor cause aliasing of the
// store's internal buffers.
package kv
