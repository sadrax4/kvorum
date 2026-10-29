// Package node is the runtime that turns the raft library into a replicated
// key-value server. It owns the single goroutine that drives raft.RawNode and
// implements the Ready contract: it persists hard state and entries to the
// write-ahead log, sends messages through the transport, applies committed
// entries to the kv.Store, serves linearizable reads through ReadIndex, takes
// snapshots, and tracks cluster membership.
//
// Everything that crosses into the run loop goes through channels, so the
// raft state machine is never touched concurrently. Results flow back to
// callers through per-request channels.
package node
