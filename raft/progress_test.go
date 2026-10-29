package raft

import "testing"

func TestInflightsWindow(t *testing.T) {
	t.Parallel()
	in := newInflights(3)
	for i := Index(1); i <= 3; i++ {
		in.add(i)
	}
	if !in.full() || in.count() != 3 {
		t.Fatalf("full=%t count=%d", in.full(), in.count())
	}
	in.freeLE(1)
	if in.full() || in.count() != 2 {
		t.Fatalf("after freeLE(1): full=%t count=%d", in.full(), in.count())
	}
	in.add(4) // wraps around the ring
	in.freeLE(3)
	if in.count() != 1 {
		t.Fatalf("after freeLE(3): count=%d", in.count())
	}
	in.freeFirstOne()
	if in.count() != 0 || in.start != 0 {
		t.Fatalf("after freeFirstOne: count=%d start=%d", in.count(), in.start)
	}
	in.freeLE(100) // empty window is a no-op
	defer func() {
		if recover() == nil {
			t.Fatal("adding to a full window did not panic")
		}
	}()
	in.add(5)
	in.add(6)
	in.add(7)
	in.add(8)
}

func TestProgressStateTransitions(t *testing.T) {
	t.Parallel()
	pr := newProgress(11, 4, false)
	if pr.State != ProgressProbe || pr.Next != 11 || !pr.RecentActive {
		t.Fatalf("initial progress = %s", pr)
	}

	// Probe: one message in flight at a time.
	pr.sentEntries(12)
	if !pr.isPaused() {
		t.Fatal("probe not paused after sending")
	}
	if !pr.maybeUpdate(12) || pr.Match != 12 || pr.Next != 13 || pr.isPaused() {
		t.Fatalf("after ack: %s", pr)
	}
	pr.becomeReplicate()
	if pr.Next != 13 {
		t.Fatalf("replicate next = %d", pr.Next)
	}

	// Replicate: optimistic window.
	for i := Index(13); i <= 16; i++ {
		pr.sentEntries(i)
	}
	if !pr.isPaused() || pr.Next != 17 {
		t.Fatalf("window not full: %s", pr)
	}
	// A stale rejection is ignored.
	if pr.maybeDecrTo(10, 5) {
		t.Fatal("stale rejection accepted in replicate state")
	}
	// A real rejection falls back to Match+1.
	if !pr.maybeDecrTo(14, 13) || pr.Next != 13 {
		t.Fatalf("after rejection: %s", pr)
	}
	pr.becomeProbe()
	if pr.State != ProgressProbe || pr.Next != 13 || pr.Inflights.count() != 0 {
		t.Fatalf("after becomeProbe: %s", pr)
	}
	// In probe state a rejection must match Next-1.
	if pr.maybeDecrTo(99, 1) {
		t.Fatal("mismatched rejection accepted in probe state")
	}
	if !pr.maybeDecrTo(12, 7) || pr.Next != 7 {
		t.Fatalf("probe rejection moved next to %d, want 7", pr.Next)
	}
	// Never below 1.
	pr.Next = 1
	if !pr.maybeDecrTo(0, 0) || pr.Next != 1 {
		t.Fatalf("next fell below 1: %d", pr.Next)
	}

	// Snapshot state resumes after the snapshot index.
	pr.becomeSnapshot(50)
	if !pr.isPaused() || pr.PendingSnapshot != 50 {
		t.Fatalf("snapshot state: %s", pr)
	}
	pr.becomeProbe()
	if pr.Next != 51 || pr.PendingSnapshot != 0 {
		t.Fatalf("after snapshot: %s", pr)
	}
}
