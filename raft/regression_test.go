package raft

import (
	"testing"
)

// Regression tests for defects found by adversarial review.

func TestReadIndexDistinguishesRequestersWithSameContext(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.mustPropose(1, "x")
	commit := nw.status(1).Commit

	// Both followers forward a read with identical context bytes, as an
	// application using per-node counters would.
	nw.node(2).rn.ReadIndex([]byte("ctx"))
	nw.node(3).rn.ReadIndex([]byte("ctx"))
	nw.flush()
	for _, id := range []NodeID{2, 3} {
		rs := nw.node(id).readStates
		if len(rs) != 1 || string(rs[0].Context) != "ctx" || rs[0].Index != commit {
			t.Fatalf("node %d read states = %+v, want one at index %d", id, rs, commit)
		}
	}
	// The leader also answers its own read with the same context.
	nw.node(1).rn.ReadIndex([]byte("ctx"))
	nw.flush()
	if rs := nw.node(1).readStates; len(rs) != 1 || rs[0].Index != commit {
		t.Fatalf("leader read states = %+v", rs)
	}
}

func TestMalformedAppendEntriesAreRejectedWithoutPanic(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.mustPropose(1, "x")
	follower := nw.node(2)
	st := nw.status(2)
	before := entriesString(follower.rn.raft.raftLog.allEntries())

	cases := []Message{
		// Entry index far below the previous index: conflicts with a committed entry.
		{Type: MsgApp, From: 1, To: 2, Term: st.Term, PrevLogIndex: st.LastIndex, PrevLogTerm: st.Term,
			Entries: []Entry{{Index: 1, Term: 99}}},
		// Gap after the previous index.
		{Type: MsgApp, From: 1, To: 2, Term: st.Term, PrevLogIndex: st.LastIndex, PrevLogTerm: st.Term,
			Entries: []Entry{{Index: st.LastIndex + 5, Term: st.Term}}},
		// Decreasing term.
		{Type: MsgApp, From: 1, To: 2, Term: st.Term, PrevLogIndex: st.LastIndex, PrevLogTerm: st.Term,
			Entries: []Entry{{Index: st.LastIndex + 1, Term: 1}}},
		// Unknown entry type.
		{Type: MsgApp, From: 1, To: 2, Term: st.Term, PrevLogIndex: st.LastIndex, PrevLogTerm: st.Term,
			Entries: []Entry{{Index: st.LastIndex + 1, Term: st.Term, Type: EntryType(7)}}},
		// Conflict with a committed entry at a matching previous position.
		{Type: MsgApp, From: 1, To: 2, Term: st.Term, PrevLogIndex: st.Commit - 1, PrevLogTerm: 1,
			Entries: []Entry{{Index: st.Commit, Term: 42}}},
	}
	for i, m := range cases {
		if err := follower.rn.Step(m); err != nil {
			t.Fatalf("case %d: step returned %v", i, err)
		}
		follower.drain(t)
		if got := entriesString(follower.rn.raft.raftLog.allEntries()); got != before {
			t.Fatalf("case %d changed the log: %s -> %s", i, before, got)
		}
	}
	// The cluster still works afterwards.
	nw.mustPropose(1, "after")
}

func TestPendingReadsReleasedWhenClusterShrinksToOneVoter(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2})
	nw.campaign(1)
	nw.mustPropose(1, "x")

	// The read's heartbeat round is lost, so it stays pending.
	nw.ignore[MsgHeartbeatResp] = true
	nw.node(1).rn.ReadIndex([]byte("stuck"))
	nw.flush()
	if rs := nw.node(1).readStates; len(rs) != 0 {
		t.Fatalf("read answered without acknowledgements: %+v", rs)
	}
	delete(nw.ignore, MsgHeartbeatResp)

	// Removing node 2 leaves node 1 as the only voter; the pending read must
	// be released even though no heartbeat response will ever arrive for it.
	nw.ignore[MsgHeartbeatResp] = true
	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 2}); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if cs := nw.status(1).ConfState; len(cs.Voters) != 1 {
		t.Fatalf("conf = %s", cs)
	}
	rs := nw.node(1).readStates
	if len(rs) != 1 || string(rs[0].Context) != "stuck" {
		t.Fatalf("pending read not released after the quorum shrank: %+v", rs)
	}
}

func TestLeaderTransferRetransmitsTimeoutNow(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.mustPropose(1, "x")

	// The first MsgTimeoutNow is lost.
	nw.ignore[MsgTimeoutNow] = true
	if err := nw.node(1).rn.TransferLeader(2); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if got := nw.leader(); got != 1 {
		t.Fatalf("leader = %d after a lost timeout-now, want 1", got)
	}
	delete(nw.ignore, MsgTimeoutNow)
	// The next heartbeat tick repeats it and the transfer completes well
	// within the election timeout.
	nw.tickAll(2)
	if got := nw.leader(); got != 2 {
		t.Fatalf("leader = %d, want 2 after retransmitted timeout-now", got)
	}
}

func TestBootstrapEntriesIdenticalAcrossNodes(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{3, 1, 2})
	want := entriesString(nw.node(1).rn.raft.raftLog.allEntries())
	for _, id := range nw.ids {
		ents := nw.node(id).rn.raft.raftLog.allEntries()
		if entriesString(ents) != want {
			t.Fatalf("node %d bootstrap log %s, want %s", id, entriesString(ents), want)
		}
		for i, e := range ents {
			ref := nw.node(1).rn.raft.raftLog.allEntries()[i]
			if string(e.Data) != string(ref.Data) {
				t.Fatalf("node %d entry %d data differs from node 1", id, e.Index)
			}
		}
	}
}
