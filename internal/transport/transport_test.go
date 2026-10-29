package transport

import (
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

func TestExchangeMsgApp(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	b := startNode(t, 2, nil)
	connect(a, b)

	msg := raft.Message{
		Type:         raft.MsgApp,
		From:         1,
		To:           2,
		Term:         3,
		PrevLogIndex: 10,
		PrevLogTerm:  2,
		Commit:       9,
		Entries: []raft.Entry{
			{Index: 11, Term: 3, Type: raft.EntryNormal, Data: []byte("set k=v")},
			{Index: 12, Term: 3, Type: raft.EntryConfChange, Data: []byte{9, 8, 7}},
		},
	}
	a.tr.Send([]raft.Message{msg})

	waitFor(t, 5*time.Second, func() bool { return len(b.handler.messages()) == 1 }, "message at b")
	if got := b.handler.messages()[0]; !reflect.DeepEqual(got, msg) {
		t.Fatalf("received %+v, want %+v", got, msg)
	}

	reply := raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, MatchIndex: 12}
	b.tr.Send([]raft.Message{reply})
	waitFor(t, 5*time.Second, func() bool { return len(a.handler.messages()) == 1 }, "reply at a")
	if got := a.handler.messages()[0]; !reflect.DeepEqual(got, reply) {
		t.Fatalf("received %+v, want %+v", got, reply)
	}

	if a.metrics.sent.Load() != 1 || a.metrics.sentBytes.Load() == 0 {
		t.Fatalf("a sent=%d bytes=%d", a.metrics.sent.Load(), a.metrics.sentBytes.Load())
	}
	if b.metrics.received.Load() != 1 || b.metrics.receivedBytes.Load() == 0 {
		t.Fatalf("b received=%d bytes=%d", b.metrics.received.Load(), b.metrics.receivedBytes.Load())
	}
	if a.handler.unreachableCount() != 0 || b.handler.unreachableCount() != 0 {
		t.Fatal("unexpected unreachable reports on a healthy link")
	}
}

func TestManyMessagesArriveInOrder(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	b := startNode(t, 2, nil)
	connect(a, b)

	const n = 200
	msgs := make([]raft.Message, n)
	for i := range msgs {
		msgs[i] = raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1, Commit: raft.Index(i)}
	}
	a.tr.Send(msgs)
	waitFor(t, 10*time.Second, func() bool { return len(b.handler.messages()) == n }, "all heartbeats")
	for i, m := range b.handler.messages() {
		if m.Commit != raft.Index(i) {
			t.Fatalf("message %d has commit %d; delivery reordered", i, m.Commit)
		}
	}
}

func TestUnknownPeerDropped(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	a.tr.Send([]raft.Message{
		{Type: raft.MsgHeartbeat, From: 1, To: 42},
		{Type: raft.MsgHeartbeat, From: 1, To: 42},
		{Type: raft.MsgSnap, From: 1, To: 43, Snapshot: &raft.Snapshot{}},
	})
	if got := a.metrics.dropped.Load(); got != 3 {
		t.Fatalf("dropped = %d, want 3", got)
	}
	if a.metrics.sendFailed.Load() != 0 || a.handler.unreachableCount() != 0 {
		t.Fatal("unknown peers must not be reported unreachable")
	}
}

func TestDeadPeerReportsUnreachableWithoutBlocking(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	a := startNode(t, 1, nil)
	a.tr.now = clock.Now
	a.tr.AddPeer(2, deadAddress(t))

	start := time.Now()
	a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Send blocked for %v", elapsed)
	}
	waitFor(t, 5*time.Second, func() bool { return a.metrics.sendFailed.Load() >= 1 }, "send failure")
	waitFor(t, 5*time.Second, func() bool { return a.handler.unreachableCount() == 1 }, "unreachable report")

	// Further failures within the same second are counted but not reported.
	for range 10 {
		a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
	}
	waitFor(t, 5*time.Second, func() bool { return a.metrics.sendFailed.Load() >= 11 }, "more send failures")
	if got := a.handler.unreachableCount(); got != 1 {
		t.Fatalf("unreachable reports = %d, want 1 (rate limited)", got)
	}

	// Once the interval elapses the next failure is reported again.
	clock.Advance(rateLimitInterval)
	a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
	waitFor(t, 5*time.Second, func() bool { return a.handler.unreachableCount() == 2 }, "second unreachable report")
	if a.handler.unreachable[0] != 2 || a.handler.unreachable[1] != 2 {
		t.Fatalf("unreachable ids = %v, want [2 2]", a.handler.unreachable)
	}
}

func TestStopWithFullQueueCompletesPromptly(t *testing.T) {
	// Not parallel: it compares goroutine counts before and after.
	warm := startNode(t, 1, nil)
	warm.tr.Stop()
	baseline := runtime.NumGoroutine()

	b := startNode(t, 2, nil)
	b.handler.block = make(chan struct{})
	b.handler.entered = make(chan struct{}, 1)
	const queue = 4
	a := startNode(t, 1, func(c *Config) {
		c.QueueSize = queue
		c.SendTimeout = time.Minute
	})
	connect(a, b)

	heartbeat := raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2}
	a.tr.Send([]raft.Message{heartbeat})
	select {
	case <-b.handler.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first message never reached b")
	}
	// The sender is parked inside the blocked call; fill the queue, then
	// overflow it.
	for range queue {
		a.tr.Send([]raft.Message{heartbeat})
	}
	if got := a.metrics.dropped.Load(); got != 0 {
		t.Fatalf("dropped %d before the queue was full", got)
	}
	a.tr.Send([]raft.Message{heartbeat, heartbeat, heartbeat})
	if got := a.metrics.dropped.Load(); got != 3 {
		t.Fatalf("dropped = %d, want 3", got)
	}

	for _, tr := range []*Transport{a.tr, b.tr} {
		start := time.Now()
		tr.Stop()
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("Stop took %v", elapsed)
		}
		tr.Stop()
	}
	waitFor(t, 5*time.Second, func() bool { return runtime.NumGoroutine() <= baseline }, "goroutines to settle")
	if a.handler.unreachableCount() != 0 {
		t.Fatal("stopping must not report peers unreachable")
	}
}

func TestAddPeerChangedAddressReconnects(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	old := startNode(t, 2, nil)
	replacement := startNode(t, 2, nil)

	a.tr.AddPeer(2, old.tr.Address())
	a.tr.AddPeer(2, old.tr.Address())
	a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2, Commit: 1}})
	waitFor(t, 5*time.Second, func() bool { return len(old.handler.messages()) == 1 }, "message at old address")

	a.tr.AddPeer(2, replacement.tr.Address())
	if got := a.tr.Peers(); !reflect.DeepEqual(got, map[raft.NodeID]string{2: replacement.tr.Address()}) {
		t.Fatalf("Peers = %v", got)
	}
	a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2, Commit: 2}})
	waitFor(t, 5*time.Second, func() bool { return len(replacement.handler.messages()) == 1 }, "message at new address")
	if len(old.handler.messages()) != 1 {
		t.Fatal("old address received a message after the change")
	}

	a.tr.RemovePeer(2)
	a.tr.RemovePeer(2)
	if got := a.tr.Peers(); len(got) != 0 {
		t.Fatalf("Peers after remove = %v", got)
	}
	a.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
	if a.metrics.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1 after removal", a.metrics.dropped.Load())
	}
}

func TestAddPeerRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	a.tr.AddPeer(raft.None, "127.0.0.1:1")
	a.tr.AddPeer(1, "127.0.0.1:1")
	a.tr.AddPeer(2, "")
	if got := a.tr.Peers(); len(got) != 0 {
		t.Fatalf("Peers = %v, want none", got)
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	tr, err := New(Config{LocalID: 1, ListenAddress: "127.0.0.1:0", Handler: &recordingHandler{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if addr := tr.Address(); addr != "" {
		t.Fatalf("Address before Start = %q", addr)
	}
	tr.AddPeer(2, "127.0.0.1:1")
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if addr := tr.Address(); addr == "" || addr == "127.0.0.1:0" {
		t.Fatalf("Address after Start = %q, want a bound port", addr)
	}
	if err := tr.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start error = %v, want ErrAlreadyStarted", err)
	}
	tr.Stop()
	tr.Stop()
	if err := tr.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Stop error = %v, want ErrStopped", err)
	}
	tr.AddPeer(3, "127.0.0.1:1")
	if got := tr.Peers(); len(got) != 0 {
		t.Fatalf("Peers after Stop = %v", got)
	}
	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
}

func TestStopWithoutStart(t *testing.T) {
	t.Parallel()
	tr, err := New(Config{LocalID: 1, ListenAddress: "127.0.0.1:0", Handler: &recordingHandler{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tr.AddPeer(2, deadAddress(t))
	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2}})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop without Start hung")
	}
}

func TestStartFailsOnBusyAddress(t *testing.T) {
	t.Parallel()
	a := startNode(t, 1, nil)
	tr, err := New(Config{LocalID: 2, ListenAddress: a.tr.Address(), Handler: &recordingHandler{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(tr.Stop)
	if err := tr.Start(); err == nil {
		t.Fatal("Start on a busy address succeeded")
	}
}

// TestSnapshotToUnknownPeerIsReportedFailed covers a MsgSnap addressed to a
// peer the transport does not know: Raft pauses replication to that follower
// until it hears about the snapshot, so the drop must be reported.
func TestSnapshotToUnknownPeerIsReportedFailed(t *testing.T) {
	t.Parallel()
	h := &recordingHandler{}
	tr, err := New(Config{LocalID: 1, ListenAddress: "127.0.0.1:0", Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Stop()
	tr.Send([]raft.Message{{Type: raft.MsgSnap, From: 1, To: 9, Term: 1, Snapshot: &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 1, Term: 1}}}})
	reports := h.snapshotReports()
	if len(reports) != 1 || reports[0].id != 9 || reports[0].succeeded {
		t.Fatalf("snapshot reports = %+v, want one failure for peer 9", reports)
	}
}
