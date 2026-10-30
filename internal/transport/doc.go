// Package transport carries [raft.Message] values between the members of a
// kvorum cluster over gRPC, implementing the RaftService defined in
// api/proto/kvorum/raft/v1/raft.proto.
//
// A [Transport] serves inbound messages on a listener and keeps one outbound
// [peer] per known member. Each peer owns a bounded queue and a single sender
// goroutine that dials the member lazily and writes queued messages to one
// long-lived Pipe stream, so ordinary traffic pays no per-message RPC
// overhead; a failed or stalled write closes the stream and the next message
// reopens it. The unary Step call remains available for single messages.
// Snapshots are too large for either, so a MsgSnap bypasses the queue and is
// streamed on a dedicated goroutine through the InstallSnapshot client
// stream: the first chunk carries the message header (the MsgSnap with its
// metadata, produced by [ToProto]) and the following chunks carry the
// snapshot payload in [Config.SnapshotChunkSize] pieces.
//
// # Delivery guarantees
//
// The transport is best effort, which is all Raft requires: it never blocks
// the caller of [Transport.Send] and never retries. A message is dropped when
// its destination is not a known peer, when the peer's queue is full, or when
// the call fails; Raft detects the gap and retransmits. Failures are reported
// to the [Handler] through ReportUnreachable, rate-limited to once per second
// per peer so that a dead member does not flood the consensus loop, and
// counted through [Metrics]. Snapshot outcomes are reported through
// ReportSnapshot exactly once per attempt.
//
// # Inbound validation
//
// A received message must be addressed to the local node (To == LocalID) and
// carry a non-zero From; otherwise it is rejected with codes.InvalidArgument.
// Decoding happens through [FromProto], which validates enumerations and
// pointers so that a malformed peer cannot crash the receiver. A reassembled
// snapshot is bounded by [Config.MaxSnapshotSize]; anything larger is rejected
// with codes.ResourceExhausted. Errors returned by Handler.Step surface to the
// sender as codes.Internal.
//
// # Lifecycle
//
// [New] validates the configuration, [Transport.Start] binds the listener and
// serves in the background, and [Transport.Stop] cancels every in-flight call,
// closes every peer connection, stops the server, and waits for all goroutines
// the transport started. Stop is idempotent and returns promptly even when
// queues are full. Peers may be added before Start and are torn down by Stop
// whether or not Start was ever called.
package transport
