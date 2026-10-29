package transport

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

// Handler receives inbound messages and delivery feedback from a [Transport].
// It is implemented by the node that drives the Raft state machine.
type Handler interface {
	// Step delivers a message received from a peer. The context is cancelled
	// when the sender abandons the call or the transport stops.
	Step(ctx context.Context, m raft.Message) error
	// ReportUnreachable signals that a message to id could not be delivered.
	// The transport rate-limits these reports to one per second per peer.
	ReportUnreachable(id raft.NodeID)
	// ReportSnapshot signals the outcome of a snapshot transfer to id.
	ReportSnapshot(id raft.NodeID, succeeded bool)
}

// Default values applied by [New] to zero fields of [Config].
const (
	// DefaultDialTimeout bounds a single connection attempt to a peer.
	DefaultDialTimeout = 3 * time.Second
	// DefaultSendTimeout bounds a single unary Step call.
	DefaultSendTimeout = 5 * time.Second
	// DefaultSnapshotChunkSize is the payload size of one InstallSnapshot chunk.
	DefaultSnapshotChunkSize = 256 << 10
	// DefaultQueueSize is the capacity of each peer's outbound queue.
	DefaultQueueSize = 4096
	// DefaultMaxMessageSize is the largest gRPC message accepted or sent.
	DefaultMaxMessageSize = 64 << 20
	// DefaultMaxSnapshotSize is the largest reassembled snapshot accepted.
	DefaultMaxSnapshotSize int64 = 4 << 30
)

// snapshotChunkOverhead is the protobuf framing added to a data chunk: the
// field tag plus a length varint. It keeps a chunk within MaxMessageSize.
const snapshotChunkOverhead = 16

// Config configures a [Transport]. Zero durations and sizes take the
// documented defaults; LocalID, ListenAddress, and Handler are required.
type Config struct {
	// LocalID is the identity of this node. Inbound messages must be
	// addressed to it.
	LocalID raft.NodeID
	// ListenAddress is the TCP address to bind, for example "0.0.0.0:2380".
	// Port 0 selects a free port; the bound address is reported by
	// [Transport.Address].
	ListenAddress string
	// Handler receives inbound messages and delivery feedback.
	Handler Handler
	// Publisher receives member attributes published by followers through
	// the Publish RPC. Nil makes the RPC answer Unimplemented.
	Publisher Publisher
	// Logger receives diagnostics. Nil discards them.
	Logger *slog.Logger
	// Metrics receives counters. Nil uses [NopMetrics].
	Metrics Metrics
	// DialTimeout bounds one connection attempt to a peer.
	DialTimeout time.Duration
	// SendTimeout bounds one unary Step call. Snapshot streams get an
	// additional allowance proportional to their size.
	SendTimeout time.Duration
	// SnapshotChunkSize is the payload size of one InstallSnapshot chunk. It
	// must leave room for framing within MaxMessageSize.
	SnapshotChunkSize int
	// QueueSize is the capacity of each peer's outbound queue. Messages that
	// do not fit are dropped.
	QueueSize int
	// MaxMessageSize is the largest gRPC message the server accepts and the
	// client sends or receives.
	MaxMessageSize int
	// MaxSnapshotSize caps the bytes of a reassembled inbound snapshot.
	MaxSnapshotSize int64
}

// withDefaults returns a copy of c with defaults filled in and nil
// dependencies replaced by no-op implementations.
func (c Config) withDefaults() Config {
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Metrics == nil {
		c.Metrics = NopMetrics{}
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = DefaultSendTimeout
	}
	if c.SnapshotChunkSize == 0 {
		c.SnapshotChunkSize = DefaultSnapshotChunkSize
	}
	if c.QueueSize == 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = DefaultMaxMessageSize
	}
	if c.MaxSnapshotSize == 0 {
		c.MaxSnapshotSize = DefaultMaxSnapshotSize
	}
	return c
}

// validate checks a configuration after defaults have been applied.
func (c Config) validate() error {
	if c.LocalID == raft.None {
		return fmt.Errorf("%w: LocalID must not be zero", ErrInvalidConfig)
	}
	if c.ListenAddress == "" {
		return fmt.Errorf("%w: ListenAddress must not be empty", ErrInvalidConfig)
	}
	if c.Handler == nil {
		return fmt.Errorf("%w: Handler must not be nil", ErrInvalidConfig)
	}
	if c.DialTimeout < 0 {
		return fmt.Errorf("%w: DialTimeout must not be negative", ErrInvalidConfig)
	}
	if c.SendTimeout < 0 {
		return fmt.Errorf("%w: SendTimeout must not be negative", ErrInvalidConfig)
	}
	if c.SnapshotChunkSize < 0 {
		return fmt.Errorf("%w: SnapshotChunkSize must not be negative", ErrInvalidConfig)
	}
	if c.QueueSize < 0 {
		return fmt.Errorf("%w: QueueSize must not be negative", ErrInvalidConfig)
	}
	if c.MaxMessageSize < 0 {
		return fmt.Errorf("%w: MaxMessageSize must not be negative", ErrInvalidConfig)
	}
	if c.MaxSnapshotSize < 0 {
		return fmt.Errorf("%w: MaxSnapshotSize must not be negative", ErrInvalidConfig)
	}
	if c.SnapshotChunkSize+snapshotChunkOverhead > c.MaxMessageSize {
		return fmt.Errorf("%w: SnapshotChunkSize %d does not fit in MaxMessageSize %d",
			ErrInvalidConfig, c.SnapshotChunkSize, c.MaxMessageSize)
	}
	return nil
}
