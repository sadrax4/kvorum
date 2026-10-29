package raft

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestLeaderElectionThreeNodes(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)

	if got := nw.leader(); got != 1 {
		t.Fatalf("leader = %d, want 1", got)
	}
	for _, id := range nw.ids {
		st := nw.status(id)
		if st.Leader != 1 {
			t.Errorf("node %d: leader = %d, want 1", id, st.Leader)
		}
		// Bootstrap is term 1; the election (after Pre-Vote) is term 2.
		if st.Term != 2 {
			t.Errorf("node %d: term = %d, want 2", id, st.Term)
		}
	}
	// The leader's no-op entry is committed on every node: 3 bootstrap
	// entries plus the no-op.
	for _, id := range nw.ids {
		if st := nw.status(id); st.Commit != 4 {
			t.Errorf("node %d: commit = %d, want 4", id, st.Commit)
		}
	}
}

func TestLeaderElectionSingleNode(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1})
	nw.campaign(1)
	if got := nw.leader(); got != 1 {
		t.Fatalf("leader = %d, want 1", got)
	}
	idx := nw.mustPropose(1, "alone")
	if st := nw.status(1); st.Commit != idx || st.Applied != idx {
		t.Fatalf("commit/applied = %d/%d, want %d", st.Commit, st.Applied, idx)
	}
	got := nw.node(1).appliedNormal()
	if len(got) != 1 || string(got[0].Data) != "alone" {
		t.Fatalf("applied = %v", got)
	}
}

func TestElectionTimeoutProducesSingleLeader(t *testing.T) {
	t.Parallel()
	for _, pv := range []bool{true, false} {
		t.Run(fmt.Sprintf("prevote=%t", pv), func(t *testing.T) {
			t.Parallel()
			var opts []networkOption
			if !pv {
				opts = append(opts, withoutPreVote())
			}
			nw := newNetwork(t, []NodeID{1, 2, 3, 4, 5}, opts...)
			lead := nw.electLeader()
			// Every node agrees on the leader once traffic settles.
			nw.tickAll(2)
			for _, id := range nw.ids {
				if st := nw.status(id); st.Leader != lead {
					t.Errorf("node %d: leader = %d, want %d", id, st.Leader, lead)
				}
			}
		})
	}
}

func TestDuelingCandidatesResolve(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	// Start two campaigns before any message is delivered.
	if err := nw.node(1).rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := nw.node(3).rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	lead := nw.electLeader()
	nw.tickAll(3)
	if nw.leader() != lead {
		t.Fatalf("leadership changed after election: %d -> %d", lead, nw.leader())
	}
}

func TestLogReplication(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	for i := range 3 {
		nw.mustPropose(1, fmt.Sprintf("cmd-%d", i))
	}
	want := nw.node(1).appliedNormal()
	if len(want) != 3 {
		t.Fatalf("leader applied %d commands, want 3", len(want))
	}
	for _, id := range nw.ids {
		got := nw.node(id).appliedNormal()
		if len(got) != len(want) {
			t.Fatalf("node %d applied %d commands, want %d", id, len(got), len(want))
		}
		for i := range want {
			if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Data, want[i].Data) {
				t.Fatalf("node %d entry %d = %v, want %v", id, i, got[i], want[i])
			}
		}
	}
}

func TestProposalOnFollowerIsRejected(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	if _, err := nw.propose(2, "nope"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose on follower: err = %v, want ErrNotLeader", err)
	}
}

func TestCommitRequiresQuorum(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	before := nw.status(1).Commit

	nw.isolate(2)
	nw.isolate(3)
	idx := nw.mustPropose(1, "lonely")
	if st := nw.status(1); st.Commit != before {
		t.Fatalf("commit advanced to %d without a quorum (was %d)", st.Commit, before)
	}

	nw.heal()
	nw.tickAll(1) // heartbeat round re-establishes contact and resends
	if st := nw.status(1); st.Commit != idx {
		t.Fatalf("commit = %d after heal, want %d", st.Commit, idx)
	}
	for _, id := range nw.ids {
		if got := nw.node(id).appliedNormal(); len(got) != 1 {
			t.Errorf("node %d applied %d commands, want 1", id, len(got))
		}
	}
}

// TestCommitOnlyThroughCurrentTermEntry covers the Figure 8 scenario: an
// entry from an earlier term that is replicated on a majority must not be
// committed by counting replicas; it becomes committed only once an entry of
// the current term is committed on top of it.
func TestCommitOnlyThroughCurrentTermEntry(t *testing.T) {
	t.Parallel()
	// CheckQuorum leases would make followers ignore node 2's campaign while
	// they are in contact with node 1; the scenario is about commitment, so
	// run without them.
	nw := newNetwork(t, []NodeID{1, 2, 3, 4, 5}, withoutCheckQuorum())
	nw.campaign(1)
	baseline := nw.status(1).Commit

	// Node 1 can only reach node 2: its proposals land on a minority.
	for _, id := range []NodeID{3, 4, 5} {
		nw.cutLink(1, id)
	}
	nw.mustPropose(1, "old-term-a")
	nw.mustPropose(1, "old-term-b")
	if st := nw.status(1); st.Commit != baseline {
		t.Fatalf("minority replication committed entries: commit %d > %d", st.Commit, baseline)
	}
	if st := nw.status(2); st.LastIndex != baseline+2 {
		t.Fatalf("node 2 last index = %d, want %d", st.LastIndex, baseline+2)
	}

	// Node 2, which has the old-term entries, becomes leader while
	// AppendEntries is suppressed, so its no-op cannot be committed yet.
	nw.heal()
	nw.ignore[MsgApp] = true
	nw.campaign(2)
	if got := nw.leader(); got != 2 {
		t.Fatalf("leader = %d, want 2", got)
	}
	if st := nw.status(2); st.Commit != baseline {
		t.Fatalf("new leader committed old-term entries without a current-term entry: %d > %d", st.Commit, baseline)
	}

	// Allow AppendEntries again: the no-op of the new term gets replicated,
	// and committing it commits the old-term entries beneath it.
	delete(nw.ignore, MsgApp)
	nw.tickAll(1)
	want := nw.status(2).LastIndex
	if st := nw.status(2); st.Commit != want {
		t.Fatalf("commit = %d, want %d", st.Commit, want)
	}
	for _, id := range nw.ids {
		got := nw.node(id).appliedNormal()
		if len(got) != 2 || string(got[0].Data) != "old-term-a" || string(got[1].Data) != "old-term-b" {
			t.Fatalf("node %d applied %v", id, got)
		}
	}
}

func TestMaybeCommitRejectsOldTermEntries(t *testing.T) {
	t.Parallel()
	storage := newTestStorage(1, 2, 3)
	// Log: entry 1 from term 1, entry 2 from term 2. The node is leader in
	// term 3 with its no-op at index 3.
	if err := storage.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	storage.SetHardState(HardState{Term: 2})
	rn := mustNewRawNode(t, testConfig(t, 1, storage, 1))
	r := rn.raft
	r.becomeCandidate()
	r.becomeLeader()
	if r.Term != 3 || r.raftLog.lastIndex() != 3 {
		t.Fatalf("term/last = %d/%d, want 3/3", r.Term, r.raftLog.lastIndex())
	}

	// A majority has entry 2 (old term) but not the term-3 no-op.
	r.prs[2].Match = 2
	if r.maybeCommit() {
		t.Fatal("committed an old-term entry by counting replicas")
	}
	if r.raftLog.committed != 0 {
		t.Fatalf("commit = %d, want 0", r.raftLog.committed)
	}

	// Once the no-op is replicated on a majority, everything commits.
	r.prs[2].Match = 3
	if !r.maybeCommit() {
		t.Fatal("did not commit the current-term entry")
	}
	if r.raftLog.committed != 3 {
		t.Fatalf("commit = %d, want 3", r.raftLog.committed)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	st := nw.status(1)
	if err := nw.node(1).rn.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: st.Term + 5}); err != nil {
		t.Fatal(err)
	}
	got := nw.status(1)
	if got.State != StateFollower || got.Term != st.Term+5 || got.Leader != None {
		t.Fatalf("after higher-term message: %+v", got)
	}
}

func TestPreVotePreventsTermInflation(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	term := nw.status(1).Term

	nw.isolate(3)
	nw.tickAll(100)
	if st := nw.status(3); st.Term != term {
		t.Fatalf("isolated node inflated its term to %d (cluster term %d)", st.Term, term)
	}
	if st := nw.status(3); st.State == StateLeader || st.State == StateCandidate {
		t.Fatalf("isolated node is %s", st.State)
	}

	nw.heal()
	nw.tickAll(3)
	if got := nw.leader(); got != 1 {
		t.Fatalf("leader after heal = %d, want 1 (no disruption)", got)
	}
	if st := nw.status(3); st.Term != term || st.Leader != 1 || st.State != StateFollower {
		t.Fatalf("rejoined node: %+v", st)
	}
}

func TestWithoutPreVoteIsolatedNodeDisrupts(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3}, withoutPreVote(), withoutCheckQuorum())
	nw.campaign(1)
	term := nw.status(1).Term

	nw.isolate(3)
	nw.tickAll(50)
	if st := nw.status(3); st.Term <= term {
		t.Fatalf("without Pre-Vote the isolated node should inflate its term; term = %d", st.Term)
	}
	nw.heal()
	nw.tickAll(20)
	// The old leader learns about the higher term and steps down: this is the
	// disruption Pre-Vote exists to prevent.
	if st := nw.status(1); st.State == StateLeader && st.Term == term {
		t.Fatalf("leader survived a higher-term node without Pre-Vote: %+v", st)
	}
}

func TestCheckQuorumLeaderStepsDown(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)

	nw.isolate(1)
	// The leader needs one full election timeout to notice the lost quorum.
	for range 25 {
		nw.tick(1)
	}
	if st := nw.status(1); st.State != StateFollower {
		t.Fatalf("isolated leader is still %s", st.State)
	}
	// Proposals are rejected while the old leader is isolated.
	if _, err := nw.propose(1, "x"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestFollowerIgnoresVoteWithinLease(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	st := nw.status(2)

	// Node 3 pretends to campaign for a higher term while node 2 has just
	// heard from the leader.
	req := Message{Type: MsgVote, From: 3, To: 2, Term: st.Term + 1, LastLogIndex: st.LastIndex, LastLogTerm: st.Term}
	if err := nw.node(2).rn.Step(req); err != nil {
		t.Fatal(err)
	}
	msgs := nw.node(2).drain(t)
	for _, m := range msgs {
		if m.Type == MsgVoteResp {
			t.Fatalf("follower answered a vote request inside the lease: %s", m)
		}
	}
	if got := nw.status(2); got.Term != st.Term {
		t.Fatalf("term changed to %d", got.Term)
	}

	// A transfer vote bypasses the lease.
	req.Transfer = true
	if err := nw.node(2).rn.Step(req); err != nil {
		t.Fatal(err)
	}
	granted := false
	for _, m := range nw.node(2).drain(t) {
		if m.Type == MsgVoteResp && !m.Reject && m.To == 3 {
			granted = true
		}
	}
	if !granted {
		t.Fatal("transfer vote was not granted")
	}
}

func TestLogConflictResolutionWithBacktracking(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.mustPropose(1, "shared")

	// Partition the leader away and let it accumulate a divergent tail.
	nw.isolate(1)
	for i := range 6 {
		nw.mustPropose(1, fmt.Sprintf("divergent-%d", i))
	}
	// The remaining majority elects a new leader (CheckQuorum eventually
	// steps node 1 down, but the election does not depend on it).
	newLeader := None
	for range 200 {
		nw.tickAll(1)
		for _, id := range []NodeID{2, 3} {
			if nw.status(id).State == StateLeader {
				newLeader = id
			}
		}
		if newLeader != None {
			break
		}
	}
	if newLeader == None {
		t.Fatal("majority did not elect a new leader")
	}
	for i := range 3 {
		nw.mustPropose(newLeader, fmt.Sprintf("new-%d", i))
	}

	// Heal and count how many rejections it takes to repair node 1's log.
	nw.heal()
	nw.delivered = nil
	nw.tickAll(3)
	rejections := 0
	for _, m := range nw.delivered {
		if m.Type == MsgAppResp && m.Reject {
			rejections++
		}
	}
	if rejections > 2 {
		t.Fatalf("log repair needed %d rejections; accelerated backtracking should need at most 2", rejections)
	}
	want := nw.node(newLeader).appliedNormal()
	for _, id := range nw.ids {
		got := nw.node(id).appliedNormal()
		if entriesString(got) != entriesString(want) {
			t.Fatalf("node %d applied %s, want %s", id, entriesString(got), entriesString(want))
		}
	}
	for _, e := range want {
		if bytes.HasPrefix(e.Data, []byte("divergent")) {
			t.Fatalf("divergent entry survived: %v", e)
		}
	}
}

func TestHeartbeatAdvancesFollowerCommit(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)

	// Suppress MsgApp so that node 3 learns about commits only through
	// heartbeats after the entry has been replicated to it.
	idx := nw.mustPropose(1, "x")
	if st := nw.status(3); st.Commit != idx {
		t.Fatalf("baseline: node 3 commit = %d, want %d", st.Commit, idx)
	}
	// Now cut the leader's appends to 3 but keep heartbeats flowing: the
	// commit index in heartbeats is capped at what 3 has acknowledged.
	nw.cut[[2]NodeID{1, 3}] = true
	idx2 := nw.mustPropose(1, "y")
	nw.heal()
	nw.ignore[MsgApp] = true
	nw.tickAll(1)
	if st := nw.status(3); st.Commit != idx {
		t.Fatalf("heartbeat moved commit beyond acknowledged entries: %d (acknowledged %d)", st.Commit, idx)
	}
	delete(nw.ignore, MsgApp)
	nw.tickAll(1)
	if st := nw.status(3); st.Commit != idx2 {
		t.Fatalf("node 3 commit = %d, want %d", st.Commit, idx2)
	}
}

func TestLeaderTransfer(t *testing.T) {
	t.Parallel()
	t.Run("to up-to-date follower", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		if err := nw.node(1).rn.TransferLeader(2); err != nil {
			t.Fatal(err)
		}
		nw.flush()
		if got := nw.leader(); got != 2 {
			t.Fatalf("leader = %d, want 2", got)
		}
		if st := nw.status(1); st.State != StateFollower {
			t.Fatalf("old leader is %s", st.State)
		}
		nw.mustPropose(2, "after-transfer")
	})

	t.Run("to lagging follower", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		nw.isolate(3)
		nw.mustPropose(1, "a")
		nw.mustPropose(1, "b")
		nw.heal()
		if err := nw.node(1).rn.TransferLeader(3); err != nil {
			t.Fatal(err)
		}
		// Proposals are refused during the transfer.
		if _, err := nw.propose(1, "during"); !errors.Is(err, ErrProposalDropped) && !errors.Is(err, ErrNotLeader) {
			t.Fatalf("propose during transfer: %v", err)
		}
		nw.flush()
		if got := nw.leader(); got != 3 {
			t.Fatalf("leader = %d, want 3", got)
		}
		want := nw.node(1).appliedNormal()
		if got := nw.node(3).appliedNormal(); entriesString(got) != entriesString(want) {
			t.Fatalf("transferee log %s, want %s", entriesString(got), entriesString(want))
		}
	})

	t.Run("times out when target unreachable", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		nw.isolate(3)
		if err := nw.node(1).rn.TransferLeader(3); err != nil {
			t.Fatal(err)
		}
		nw.flush()
		nw.tickAll(12)
		if got := nw.leader(); got != 1 {
			t.Fatalf("leader = %d, want 1", got)
		}
		if nw.node(1).rn.raft.leadTransferee != None {
			t.Fatal("transfer still pending after timeout")
		}
		nw.mustPropose(1, "after-abort")
	})

	t.Run("rejected on follower and for learner", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		if err := nw.node(2).rn.TransferLeader(3); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("err = %v, want ErrNotLeader", err)
		}
		if err := nw.node(1).rn.TransferLeader(9); !errors.Is(err, ErrUnknownNode) {
			t.Fatalf("err = %v, want ErrUnknownNode", err)
		}
	})
}

func TestReadIndex(t *testing.T) {
	t.Parallel()
	t.Run("leader", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		nw.mustPropose(1, "x")
		commit := nw.status(1).Commit
		nw.node(1).rn.ReadIndex([]byte("ctx-1"))
		nw.flush()
		rs := nw.node(1).readStates
		if len(rs) != 1 || string(rs[0].Context) != "ctx-1" || rs[0].Index != commit {
			t.Fatalf("read states = %+v, want index %d", rs, commit)
		}
	})

	t.Run("follower forwards to leader", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		nw.mustPropose(1, "x")
		commit := nw.status(1).Commit
		nw.node(3).rn.ReadIndex([]byte("ctx-f"))
		nw.flush()
		rs := nw.node(3).readStates
		if len(rs) != 1 || string(rs[0].Context) != "ctx-f" || rs[0].Index != commit {
			t.Fatalf("read states = %+v, want index %d", rs, commit)
		}
	})

	t.Run("waits for current-term commit", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.ignore[MsgApp] = true
		nw.campaign(1)
		if got := nw.leader(); got != 1 {
			t.Fatalf("leader = %d", got)
		}
		nw.node(1).rn.ReadIndex([]byte("early"))
		nw.flush()
		if rs := nw.node(1).readStates; len(rs) != 0 {
			t.Fatalf("read served before the leader committed in its term: %+v", rs)
		}
		delete(nw.ignore, MsgApp)
		nw.tickAll(2)
		rs := nw.node(1).readStates
		if len(rs) != 1 || string(rs[0].Context) != "early" || rs[0].Index != nw.status(1).Commit {
			t.Fatalf("read states = %+v, commit %d", rs, nw.status(1).Commit)
		}
	})

	t.Run("single node", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1})
		nw.campaign(1)
		nw.node(1).rn.ReadIndex([]byte("solo"))
		nw.flush()
		if rs := nw.node(1).readStates; len(rs) != 1 || rs[0].Index != nw.status(1).Commit {
			t.Fatalf("read states = %+v", rs)
		}
	})

	t.Run("dropped without quorum", func(t *testing.T) {
		t.Parallel()
		nw := newNetwork(t, []NodeID{1, 2, 3})
		nw.campaign(1)
		nw.isolate(2)
		nw.isolate(3)
		nw.node(1).rn.ReadIndex([]byte("no-quorum"))
		nw.flush()
		if rs := nw.node(1).readStates; len(rs) != 0 {
			t.Fatalf("read confirmed without a quorum: %+v", rs)
		}
	})
}

func TestSnapshotInstallation(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.isolate(3)
	for i := range 10 {
		nw.mustPropose(1, fmt.Sprintf("v%d", i))
	}
	// Compact the leader's log so that node 3 can only catch up via snapshot.
	nw.node(1).snapshotApplied(t, 2)
	nw.node(2).snapshotApplied(t, 2)
	leaderSnap, _ := nw.node(1).storage.Snapshot()
	if leaderSnap.IsEmpty() {
		t.Fatal("leader has no snapshot")
	}

	nw.heal()
	nw.delivered = nil
	nw.tickAll(3)

	sent := false
	for _, m := range nw.delivered {
		if m.Type == MsgSnap && m.To == 3 {
			sent = true
		}
	}
	if !sent {
		t.Fatal("leader did not send a snapshot to the lagging follower")
	}
	snap, _ := nw.node(3).storage.Snapshot()
	if snap.Meta.Index != leaderSnap.Meta.Index || snap.Meta.Term != leaderSnap.Meta.Term {
		t.Fatalf("follower snapshot %d/%d, want %d/%d", snap.Meta.Term, snap.Meta.Index, leaderSnap.Meta.Term, leaderSnap.Meta.Index)
	}
	want := nw.node(1).appliedNormal()
	if got := nw.node(3).appliedNormal(); entriesString(got) != entriesString(want) {
		t.Fatalf("follower applied %s, want %s", entriesString(got), entriesString(want))
	}

	// Replication continues normally afterwards.
	idx := nw.mustPropose(1, "after-snapshot")
	if st := nw.status(3); st.Commit != idx || st.Applied != idx {
		t.Fatalf("follower commit/applied = %d/%d, want %d", st.Commit, st.Applied, idx)
	}
	if pr := nw.status(1).Progress[3]; pr.State != ProgressReplicate || pr.Match != idx {
		t.Fatalf("progress of 3 = %+v", pr)
	}
}

func TestReportSnapshotFailureResumesProbing(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.isolate(3)
	for i := range 5 {
		nw.mustPropose(1, fmt.Sprintf("v%d", i))
	}
	nw.node(1).snapshotApplied(t, 1)
	// Keep node 3 isolated but let the leader believe it is active, so that
	// it attempts a snapshot which the "transport" then reports as failed.
	nw.node(1).rn.ReportUnreachable(3)
	nw.node(1).rn.raft.prs[3].RecentActive = true
	nw.node(1).rn.raft.sendAppend(3)
	nw.node(1).drain(t)
	if pr := nw.status(1).Progress[3]; pr.State != ProgressSnapshot {
		t.Fatalf("progress = %+v, want snapshot state", pr)
	}
	nw.node(1).rn.ReportSnapshot(3, false)
	if pr := nw.status(1).Progress[3]; pr.State != ProgressProbe {
		t.Fatalf("progress after failed snapshot = %+v, want probe", pr)
	}
	nw.heal()
	nw.tickAll(3)
	want := nw.node(1).appliedNormal()
	if got := nw.node(3).appliedNormal(); entriesString(got) != entriesString(want) {
		t.Fatalf("follower applied %s, want %s", entriesString(got), entriesString(want))
	}
}

func TestMembershipChanges(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	nw.mustPropose(1, "before")

	// Node 4 starts empty and waits to be added.
	nw.ids = append(nw.ids, 4)
	nw.nodes[4] = newAppNode(t, 4, NewMemoryStorage(), 7, nil)

	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4, Context: []byte("addr-4")}); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	nw.tickAll(2)
	if cs := nw.status(1).ConfState; !cs.IsLearner(4) {
		t.Fatalf("conf = %s, want 4 as learner", cs)
	}
	if st := nw.status(4); !st.ConfState.IsLearner(4) || st.Applied != nw.status(1).Applied {
		t.Fatalf("learner status = %+v (leader applied %d)", st, nw.status(1).Applied)
	}
	if err := nw.node(4).rn.Campaign(); !errors.Is(err, ErrLearnerCannotCampaign) {
		t.Fatalf("learner campaign: err = %v", err)
	}
	// Learners do not count towards commitment: isolating the learner does
	// not block proposals.
	nw.isolate(4)
	nw.mustPropose(1, "while-learner-isolated")
	nw.heal()
	nw.tickAll(1)

	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 4}); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if cs := nw.status(1).ConfState; !cs.IsVoter(4) || cs.IsLearner(4) {
		t.Fatalf("conf = %s, want 4 as voter", cs)
	}
	if st := nw.status(4); !st.ConfState.IsVoter(4) {
		t.Fatalf("node 4 conf = %s", st.ConfState)
	}

	// Only one change may be in flight.
	nw.ignore[MsgApp] = true
	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 2}); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second change: err = %v, want ErrConfChangePending", err)
	}
	delete(nw.ignore, MsgApp)
	nw.tickAll(2)
	if cs := nw.status(1).ConfState; cs.Contains(3) || len(cs.Voters) != 3 {
		t.Fatalf("conf after removing 3 = %s", cs)
	}

	// Removing the leader makes it step down; the rest elect a new leader.
	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	nw.flush()
	if st := nw.status(1); st.State == StateLeader {
		t.Fatal("removed leader did not step down")
	}
	lead := nw.electLeader()
	if lead == 1 || lead == 3 {
		t.Fatalf("removed node %d became leader", lead)
	}
	nw.mustPropose(lead, "after-removal")
	for _, id := range []NodeID{2, 4} {
		if st := nw.status(id); st.ConfState.Contains(1) || st.ConfState.Contains(3) {
			t.Fatalf("node %d conf = %s", id, st.ConfState)
		}
	}
	// The removed node never disrupts the cluster.
	nw.tickAll(50)
	if got := nw.leader(); got != lead {
		t.Fatalf("leader changed to %d after removed nodes kept ticking", got)
	}
}

func TestConfChangeValidation(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	// Before the leader commits an entry of its own term, changes are
	// refused.
	nw.ignore[MsgApp] = true
	nw.campaign(1)
	if _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4}); !errors.Is(err, ErrLeaderTermNotCommitted) && !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("err = %v, want ErrLeaderTermNotCommitted or ErrConfChangePending", err)
	}
	delete(nw.ignore, MsgApp)
	nw.tickAll(1)

	cases := []struct {
		name string
		cc   ConfChange
	}{
		{"add existing voter", ConfChange{Type: ConfChangeAddVoter, NodeID: 2}},
		{"add learner that is a voter", ConfChange{Type: ConfChangeAddLearner, NodeID: 2}},
		{"remove unknown", ConfChange{Type: ConfChangeRemoveNode, NodeID: 9}},
		{"unknown type", ConfChange{Type: ConfChangeType(99), NodeID: 9}},
		{"zero id", ConfChange{Type: ConfChangeAddLearner, NodeID: 0}},
	}
	for _, tc := range cases {
		if _, err := nw.node(1).rn.ProposeConfChange(tc.cc); !errors.Is(err, ErrInvalidConfChange) {
			t.Errorf("%s: err = %v, want ErrInvalidConfChange", tc.name, err)
		}
	}
	if _, err := nw.node(2).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4}); !errors.Is(err, ErrNotLeader) {
		t.Errorf("on follower: err = %v, want ErrNotLeader", err)
	}

	// Removing the last voter is refused.
	single := newNetwork(t, []NodeID{1})
	single.campaign(1)
	if _, err := single.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Errorf("remove last voter: err = %v", err)
	}
}

func TestRestartRecoversState(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	for i := range 5 {
		nw.mustPropose(1, fmt.Sprintf("v%d", i))
	}
	before := nw.status(3)
	storage := nw.node(3).storage

	nw.crash(3)
	nw.mustPropose(1, "while-down")
	nw.restart(3, storage)
	after := nw.status(3)
	if after.Term != before.Term || after.Vote != before.Vote {
		t.Fatalf("hard state after restart %+v, before %+v", after, before)
	}
	if after.Commit != before.Commit {
		t.Fatalf("commit after restart = %d, want %d", after.Commit, before.Commit)
	}
	// Committed entries are replayed to the application after restart.
	nw.flush()
	nw.tickAll(2)
	want := nw.node(1).appliedNormal()
	if got := nw.node(3).appliedNormal(); entriesString(got) != entriesString(want) {
		t.Fatalf("after restart applied %s, want %s", entriesString(got), entriesString(want))
	}
}

func TestRestartFromSnapshot(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	for i := range 6 {
		nw.mustPropose(1, fmt.Sprintf("v%d", i))
	}
	nw.node(3).snapshotApplied(t, 2)
	nw.mustPropose(1, "after-snapshot")
	storage := nw.node(3).storage
	snap, _ := storage.Snapshot()

	nw.crash(3)
	nw.restart(3, storage)
	st := nw.status(3)
	if st.Applied != snap.Meta.Index {
		t.Fatalf("applied after restart = %d, want snapshot index %d", st.Applied, snap.Meta.Index)
	}
	nw.flush()
	want := nw.node(1).appliedNormal()
	if got := nw.node(3).appliedNormal(); entriesString(got) != entriesString(want) {
		t.Fatalf("after restart applied %s, want %s", entriesString(got), entriesString(want))
	}
}

func TestBootstrapRejectsExistingState(t *testing.T) {
	t.Parallel()
	storage := NewMemoryStorage()
	rn := mustNewRawNode(t, testConfig(t, 1, storage, 1))
	if err := rn.Bootstrap(nil); err == nil {
		t.Fatal("bootstrap with no peers succeeded")
	}
	if err := rn.Bootstrap([]Peer{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err == nil {
		t.Fatal("second bootstrap succeeded")
	}
	rd := rn.Ready()
	if len(rd.Entries) != 2 || rd.HardState.Term != 1 || rd.HardState.Commit != 2 || !rd.MustSync {
		t.Fatalf("bootstrap ready = %+v", rd)
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatalf("bootstrap entries delivered for apply before being stable: %v", rd.CommittedEntries)
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	storage.SetHardState(rd.HardState)
	rn.Advance(rd)
	rd = rn.Ready()
	if len(rd.CommittedEntries) != 2 {
		t.Fatalf("committed entries after advance = %v", rd.CommittedEntries)
	}
	rn.Advance(rd)

	// Bootstrapping a second node from a non-empty storage fails.
	rn2 := mustNewRawNode(t, testConfig(t, 2, storage, 1))
	if err := rn2.Bootstrap([]Peer{{ID: 1}, {ID: 2}}); err == nil {
		t.Fatal("bootstrap on non-empty storage succeeded")
	}
}

func TestStepRejectsLocalAndUnknownPeers(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	rn := nw.node(1).rn
	if err := rn.Step(Message{Type: MsgAppResp, From: 9, To: 1, Term: 2}); !errors.Is(err, ErrStepPeerNotFound) {
		t.Errorf("response from unknown peer: err = %v", err)
	}
	if err := rn.Step(Message{Type: MsgApp, From: 2, To: 1}); !errors.Is(err, ErrStepLocalMsg) {
		t.Errorf("message without term: err = %v", err)
	}
}

func TestReadyMustSync(t *testing.T) {
	t.Parallel()
	nw := newNetwork(t, []NodeID{1, 2, 3})
	nw.campaign(1)
	leader := nw.node(1)

	// A proposal creates entries: sync required.
	if _, err := leader.rn.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rd := leader.rn.Ready()
	if !rd.MustSync || len(rd.Entries) != 1 {
		t.Fatalf("proposal ready: must_sync=%t entries=%d", rd.MustSync, len(rd.Entries))
	}
	if err := leader.storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	leader.storage.SetHardState(rd.HardState)
	leader.rn.Advance(rd)
	nw.deliver(rd.Messages)
	resps := make([]Message, 0, 2)
	for _, id := range []NodeID{2, 3} {
		resps = append(resps, nw.node(id).drain(t)...)
	}
	nw.deliver(resps)

	// Acknowledgements only move the commit index: no sync required.
	rd = leader.rn.Ready()
	if rd.MustSync {
		t.Fatalf("commit-only ready demanded a sync: %+v", rd.HardState)
	}
	if rd.HardState.IsEmpty() || rd.HardState.Commit == 0 {
		t.Fatalf("commit-only ready has no hard state: %+v", rd)
	}
	leader.rn.Advance(rd)
}

func TestLearnerDoesNotCountForQuorum(t *testing.T) {
	t.Parallel()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(Snapshot{Meta: SnapshotMeta{ConfState: ConfState{Voters: []NodeID{1, 2, 3}, Learners: []NodeID{4}}}}); err != nil {
		t.Fatal(err)
	}
	rn := mustNewRawNode(t, testConfig(t, 1, storage, 1))
	r := rn.raft
	if r.quorum() != 2 {
		t.Fatalf("quorum = %d, want 2", r.quorum())
	}
	r.becomeCandidate()
	r.becomeLeader()
	r.prs[4].Match = r.raftLog.lastIndex()
	if r.maybeCommit() {
		t.Fatal("learner acknowledgement committed an entry")
	}
	r.prs[2].Match = r.raftLog.lastIndex()
	if !r.maybeCommit() {
		t.Fatal("voter acknowledgement did not commit")
	}
}
