package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

// snapshotReport is one ReportSnapshot call observed by a recordingHandler.
type snapshotReport struct {
	id        raft.NodeID
	succeeded bool
}

// recordingHandler is a Handler double that records every call.
type recordingHandler struct {
	mu          sync.Mutex
	msgs        []raft.Message
	unreachable []raft.NodeID
	snapshots   []snapshotReport
	stepErr     error
	// block, when non-nil, makes Step wait for the context or the channel.
	block chan struct{}
	// entered receives a value each time Step starts; buffered by tests.
	entered chan struct{}
}

func (h *recordingHandler) Step(ctx context.Context, m raft.Message) error {
	if h.entered != nil {
		select {
		case h.entered <- struct{}{}:
		default:
		}
	}
	if h.block != nil {
		select {
		case <-h.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, m)
	return h.stepErr
}

func (h *recordingHandler) ReportUnreachable(id raft.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unreachable = append(h.unreachable, id)
}

func (h *recordingHandler) ReportSnapshot(id raft.NodeID, succeeded bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshots = append(h.snapshots, snapshotReport{id: id, succeeded: succeeded})
}

func (h *recordingHandler) messages() []raft.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]raft.Message(nil), h.msgs...)
}

func (h *recordingHandler) unreachableCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.unreachable)
}

func (h *recordingHandler) snapshotReports() []snapshotReport {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]snapshotReport(nil), h.snapshots...)
}

// countingMetrics is a Metrics double backed by atomic counters.
type countingMetrics struct {
	sent, sentBytes, received, receivedBytes atomic.Int64
	sendFailed, dropped                      atomic.Int64
	snapshotsOK, snapshotsFailed             atomic.Int64
}

func (m *countingMetrics) MessageSent(_ raft.MessageType, n int) {
	m.sent.Add(1)
	m.sentBytes.Add(int64(n))
}

func (m *countingMetrics) MessageReceived(_ raft.MessageType, n int) {
	m.received.Add(1)
	m.receivedBytes.Add(int64(n))
}

func (m *countingMetrics) SendFailed(raft.NodeID) { m.sendFailed.Add(1) }

func (m *countingMetrics) Dropped(raft.NodeID) { m.dropped.Add(1) }

func (m *countingMetrics) SnapshotSent(_ raft.NodeID, ok bool) {
	if ok {
		m.snapshotsOK.Add(1)
		return
	}
	m.snapshotsFailed.Add(1)
}

// fakeClock is a manually advanced clock for rate-limiter tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// testNode bundles a started transport with its doubles.
type testNode struct {
	id      raft.NodeID
	tr      *Transport
	handler *recordingHandler
	metrics *countingMetrics
}

// startNode creates and starts a transport on a free loopback port. The
// returned node is stopped when the test ends. mutate may adjust the config.
func startNode(t *testing.T, id raft.NodeID, mutate func(*Config)) *testNode {
	t.Helper()
	n := &testNode{id: id, handler: &recordingHandler{}, metrics: &countingMetrics{}}
	cfg := Config{
		LocalID:       id,
		ListenAddress: "127.0.0.1:0",
		Handler:       n.handler,
		Metrics:       n.metrics,
		SendTimeout:   2 * time.Second,
		DialTimeout:   time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	tr, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(tr.Stop)
	n.tr = tr
	return n
}

// connect makes every node a peer of every other node.
func connect(nodes ...*testNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.tr.AddPeer(b.id, b.tr.Address())
			}
		}
	}
}

// deadAddress returns a loopback address on which nothing listens.
func deadAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// rawClient dials addr directly, bypassing the Transport's peer machinery.
func rawClient(t *testing.T, addr string) raftpb.RaftServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftpb.NewRaftServiceClient(conn)
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}
