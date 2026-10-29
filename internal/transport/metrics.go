package transport

import "github.com/sadrax4/kvorum/raft"

// Metrics receives transport counters. The node wires an implementation
// backed by its Prometheus registry; [NopMetrics] discards everything.
// Implementations must be safe for concurrent use.
type Metrics interface {
	// MessageSent records a message of type t delivered to a peer, with its
	// encoded size in bytes.
	MessageSent(t raft.MessageType, bytes int)
	// MessageReceived records a message of type t accepted from a peer, with
	// its encoded size in bytes.
	MessageReceived(t raft.MessageType, bytes int)
	// SendFailed records a failed delivery attempt to the peer.
	SendFailed(to raft.NodeID)
	// Dropped records a message discarded before any delivery attempt, for
	// example because the destination is unknown or its queue is full.
	Dropped(to raft.NodeID)
	// SnapshotSent records the outcome of a snapshot transfer to the peer.
	SnapshotSent(to raft.NodeID, succeeded bool)
}

// NopMetrics is a [Metrics] implementation that discards every observation.
type NopMetrics struct{}

var _ Metrics = NopMetrics{}

// MessageSent implements [Metrics].
func (NopMetrics) MessageSent(raft.MessageType, int) {}

// MessageReceived implements [Metrics].
func (NopMetrics) MessageReceived(raft.MessageType, int) {}

// SendFailed implements [Metrics].
func (NopMetrics) SendFailed(raft.NodeID) {}

// Dropped implements [Metrics].
func (NopMetrics) Dropped(raft.NodeID) {}

// SnapshotSent implements [Metrics].
func (NopMetrics) SnapshotSent(raft.NodeID, bool) {}
