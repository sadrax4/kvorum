package node

import (
	"context"
	"fmt"
	"strconv"

	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/raft"
)

// transportMetrics adapts the shared telemetry collectors to the transport's
// metrics interface.
type transportMetrics struct {
	m *telemetry.Metrics
}

// MessageSent implements transport.Metrics.
func (t *transportMetrics) MessageSent(mt raft.MessageType, bytes int) {
	t.m.TransportMessagesSent.WithLabelValues(mt.String()).Inc()
	t.m.TransportBytesSent.Add(float64(bytes))
	t.m.RaftMessagesSent.WithLabelValues(mt.String()).Inc()
}

// MessageReceived implements transport.Metrics.
func (t *transportMetrics) MessageReceived(mt raft.MessageType, _ int) {
	t.m.RaftMessagesReceived.WithLabelValues(mt.String()).Inc()
}

// SendFailed implements transport.Metrics.
func (t *transportMetrics) SendFailed(to raft.NodeID) {
	t.m.TransportSendFailures.WithLabelValues(strconv.FormatUint(uint64(to), 10)).Inc()
}

// Dropped implements transport.Metrics.
func (t *transportMetrics) Dropped(to raft.NodeID) {
	t.m.TransportDropped.WithLabelValues(strconv.FormatUint(uint64(to), 10)).Inc()
}

// SnapshotSent implements transport.Metrics.
func (t *transportMetrics) SnapshotSent(_ raft.NodeID, succeeded bool) {
	result := "ok"
	if !succeeded {
		result = "failed"
	}
	t.m.TransportSnapshots.WithLabelValues(result).Inc()
	if succeeded {
		t.m.RaftSnapshots.WithLabelValues("sent").Inc()
	}
}

// transportHandler is the Node's implementation of transport.Handler. It is
// a distinct type so that the handler methods do not clutter Node's API.
type transportHandler Node

// Step hands an inbound Raft message to the run loop. It blocks while the
// inbound queue is full, which applies back-pressure to the sender.
func (h *transportHandler) Step(ctx context.Context, m raft.Message) error {
	n := (*Node)(h)
	select {
	case n.msgC <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
}

// ReportUnreachable records that a peer could not be reached. The report is
// dropped when the feedback queue is full; raft retransmits anyway.
func (h *transportHandler) ReportUnreachable(id raft.NodeID) {
	n := (*Node)(h)
	select {
	case n.reportC <- report{id: id}:
	default:
	}
}

// ReportSnapshot records the outcome of a snapshot transfer.
func (h *transportHandler) ReportSnapshot(id raft.NodeID, succeeded bool) {
	n := (*Node)(h)
	select {
	case n.reportC <- report{id: id, snapshot: true, ok: succeeded}:
	case <-n.stopC:
	}
}

// attributePublisher is the Node's implementation of transport.Publisher.
type attributePublisher Node

// PublishAttributes records a member's client address in the replicated
// membership table. It succeeds only on the leader.
func (p *attributePublisher) PublishAttributes(ctx context.Context, id raft.NodeID, clientAddress string) error {
	n := (*Node)(p)
	if len(clientAddress) > maxAddressLength {
		return fmt.Errorf("%w: client address exceeds %d bytes", ErrInvalidMember, maxAddressLength)
	}
	n.mu.RLock()
	member := n.confState.Contains(id)
	n.mu.RUnlock()
	if !member {
		return fmt.Errorf("%w: node %d is not a member", ErrInvalidMember, id)
	}
	prop := newProposal(ctx, proposalAttrs)
	prop.data = encodeEntry(entryKindAttrs, encodeMemberAttrs(memberAttrs{ID: id, ClientAddress: clientAddress}))
	_, err := n.await(prop)
	return err
}

// refreshStatusLocked mirrors the raft status into the fields read by other
// goroutines and into the metrics. Callers hold n.mu.
func (n *Node) refreshStatusLocked(bs raft.BasicStatus) {
	n.term = bs.Term
	n.state = bs.State
	n.leader = bs.Leader
	n.lastIndex = bs.LastIndex
	n.commitIndex.Store(uint64(bs.Commit))
	m := n.metrics
	m.RaftTerm.Set(float64(bs.Term))
	m.RaftCommitIndex.Set(float64(bs.Commit))
	m.RaftAppliedIndex.Set(float64(bs.Applied))
	m.RaftLastIndex.Set(float64(bs.LastIndex))
	m.RaftState.Set(float64(bs.State))
	if bs.State == raft.StateLeader {
		m.RaftIsLeader.Set(1)
	} else {
		m.RaftIsLeader.Set(0)
	}
}
