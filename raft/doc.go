// Package raft implements the Raft consensus algorithm as a deterministic,
// single-threaded state machine.
//
// The package deliberately owns no goroutines, timers, sockets, or files. The
// application drives a [RawNode] by feeding it clock ticks ([RawNode.Tick]),
// inbound messages ([RawNode.Step]), and proposals ([RawNode.Propose]); the
// node answers with a [Ready] that describes what must be persisted, what must
// be sent to peers, and what has been committed and can be applied. Once the
// application has acted on a Ready it calls [RawNode.Advance].
//
// This design, borrowed from etcd and refined here, has two important
// consequences:
//
//   - Every behaviour of the algorithm can be exercised deterministically in a
//     unit test or a simulator, including arbitrary message loss, reordering,
//     partitions, and crashes, without sleeping or relying on wall-clock time.
//   - Durability and networking policy belong to the application. The library
//     only states the ordering invariants it needs: entries and hard state in a
//     Ready must be stable before the Ready's messages are sent, and a snapshot
//     must be stable before the entries that follow it are applied.
//
// Implemented features:
//
//   - Leader election with randomized timeouts, Pre-Vote (§9.6 of the Raft
//     dissertation) and CheckQuorum, which together prevent partitioned nodes
//     from disrupting a healthy cluster.
//   - Log replication with pipelining, flow control, and accelerated conflict
//     resolution (the term-based backtracking optimisation from §5.3 of the
//     paper).
//   - The Figure 8 commit rule: a leader only advances the commit index through
//     an entry of its own term.
//   - Snapshot installation for followers that have fallen behind a compacted
//     log.
//   - Single-server membership changes with learners, guarded by the "one
//     change at a time" rule and the requirement that a leader commits an entry
//     of its own term before proposing a change.
//   - Linearizable reads through the ReadIndex protocol, including forwarding
//     from followers.
//   - Leadership transfer via TimeoutNow.
//
// Terminology follows the Raft paper (Ongaro & Ousterhout, "In Search of an
// Understandable Consensus Algorithm", 2014) and Ongaro's dissertation.
package raft
