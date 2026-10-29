package transport

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

func randomPayload(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0xabcdef))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	return b
}

func snapshotMessage(data []byte) raft.Message {
	return raft.Message{
		Type: raft.MsgSnap,
		From: 1,
		To:   2,
		Term: 5,
		Snapshot: &raft.Snapshot{
			Meta: raft.SnapshotMeta{
				Index:     1000,
				Term:      4,
				ConfState: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{4}},
			},
			Data: data,
		},
	}
}

func TestSnapshotStreamedAndReassembled(t *testing.T) {
	t.Parallel()
	const size = 3<<20 + 12345
	a := startNode(t, 1, func(c *Config) { c.SnapshotChunkSize = 64 << 10 })
	b := startNode(t, 2, nil)
	connect(a, b)

	data := randomPayload(1, size)
	msg := snapshotMessage(data)
	a.tr.Send([]raft.Message{msg})

	waitFor(t, 20*time.Second, func() bool { return len(b.handler.messages()) == 1 }, "snapshot at b")
	got := b.handler.messages()[0]
	if got.Type != raft.MsgSnap || got.From != 1 || got.To != 2 || got.Term != 5 {
		t.Fatalf("header mismatch: %+v", got)
	}
	if got.Snapshot == nil {
		t.Fatal("snapshot missing")
	}
	if !got.Snapshot.Meta.ConfState.Equal(msg.Snapshot.Meta.ConfState) ||
		got.Snapshot.Meta.Index != 1000 || got.Snapshot.Meta.Term != 4 {
		t.Fatalf("meta mismatch: %+v", got.Snapshot.Meta)
	}
	if !bytes.Equal(got.Snapshot.Data, data) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got.Snapshot.Data), len(data))
	}

	waitFor(t, 5*time.Second, func() bool { return len(a.handler.snapshotReports()) == 1 }, "snapshot report")
	if reports := a.handler.snapshotReports(); reports[0] != (snapshotReport{id: 2, succeeded: true}) {
		t.Fatalf("reports = %+v", reports)
	}
	if a.metrics.snapshotsOK.Load() != 1 || a.metrics.snapshotsFailed.Load() != 0 {
		t.Fatalf("snapshot metrics ok=%d failed=%d", a.metrics.snapshotsOK.Load(), a.metrics.snapshotsFailed.Load())
	}
	if n := a.metrics.sentBytes.Load(); n < size {
		t.Fatalf("sent bytes = %d, want at least %d", n, size)
	}
	if n := b.metrics.receivedBytes.Load(); n < size {
		t.Fatalf("received bytes = %d, want at least %d", n, size)
	}
}

func TestEmptySnapshotPayload(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	b := startNode(t, 2, nil)
	connect(a, b)
	a.tr.Send([]raft.Message{snapshotMessage(nil)})
	waitFor(t, 5*time.Second, func() bool { return len(b.handler.messages()) == 1 }, "snapshot at b")
	if got := b.handler.messages()[0]; got.Snapshot == nil || len(got.Snapshot.Data) != 0 {
		t.Fatalf("unexpected snapshot: %+v", got.Snapshot)
	}
	waitFor(t, 5*time.Second, func() bool { return a.metrics.snapshotsOK.Load() == 1 }, "success metric")
}

func TestOversizedSnapshotRejected(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, func(c *Config) { c.SnapshotChunkSize = 64 << 10 })
	b := startNode(t, 2, func(c *Config) { c.MaxSnapshotSize = 1 << 20 })
	connect(a, b)

	a.tr.Send([]raft.Message{snapshotMessage(randomPayload(2, 2<<20))})
	waitFor(t, 20*time.Second, func() bool { return len(a.handler.snapshotReports()) == 1 }, "snapshot report")
	if reports := a.handler.snapshotReports(); reports[0] != (snapshotReport{id: 2, succeeded: false}) {
		t.Fatalf("reports = %+v", reports)
	}
	if a.metrics.snapshotsFailed.Load() != 1 || a.metrics.sendFailed.Load() != 1 {
		t.Fatalf("failed=%d sendFailed=%d", a.metrics.snapshotsFailed.Load(), a.metrics.sendFailed.Load())
	}
	if len(b.handler.messages()) != 0 {
		t.Fatal("oversized snapshot reached the handler")
	}
}

func TestSnapshotToDeadPeerFails(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	a.tr.AddPeer(2, deadAddress(t))
	a.tr.Send([]raft.Message{snapshotMessage([]byte("x"))})
	waitFor(t, 10*time.Second, func() bool { return len(a.handler.snapshotReports()) == 1 }, "snapshot report")
	if reports := a.handler.snapshotReports(); reports[0].succeeded {
		t.Fatalf("reports = %+v", reports)
	}
	waitFor(t, 5*time.Second, func() bool { return a.handler.unreachableCount() == 1 }, "unreachable report")
}

func TestSnapshotWithoutPayloadIsDropped(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	b := startNode(t, 2, nil)
	connect(a, b)
	a.tr.Send([]raft.Message{{Type: raft.MsgSnap, From: 1, To: 2}})
	if a.metrics.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1", a.metrics.dropped.Load())
	}
	if reports := a.handler.snapshotReports(); len(reports) != 1 || reports[0].succeeded {
		t.Fatalf("reports = %+v, want one failure", reports)
	}
}

func TestConcurrentSnapshotToSamePeerIsDropped(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, nil)
	b.handler.block = make(chan struct{})
	b.handler.entered = make(chan struct{}, 1)
	a := startNode(t, 1, nil)
	connect(a, b)

	a.tr.Send([]raft.Message{snapshotMessage([]byte("first"))})
	select {
	case <-b.handler.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first snapshot never reached b")
	}
	a.tr.Send([]raft.Message{snapshotMessage([]byte("second"))})
	if a.metrics.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1", a.metrics.dropped.Load())
	}
	close(b.handler.block)
	waitFor(t, 5*time.Second, func() bool { return a.metrics.snapshotsOK.Load() == 1 }, "first snapshot to finish")
	if reports := a.handler.snapshotReports(); len(reports) != 1 || !reports[0].succeeded {
		t.Fatalf("reports = %+v", reports)
	}
}
