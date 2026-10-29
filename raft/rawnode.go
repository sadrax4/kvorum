package raft

import (
	"errors"
	"fmt"
)

// RawNode is the application-facing handle to the Raft state machine. It is
// not safe for concurrent use: the application must call all methods from a
// single goroutine (or guard them with its own lock).
//
// The expected driving loop is:
//
//	for {
//	    select {
//	    case <-ticker.C:          rn.Tick()
//	    case m := <-inbound:      rn.Step(m)
//	    case p := <-proposals:    rn.Propose(p)
//	    }
//	    if rn.HasReady() {
//	        rd := rn.Ready()
//	        // persist rd.Snapshot, rd.Entries, rd.HardState
//	        // send rd.Messages
//	        // apply rd.CommittedEntries, serve rd.ReadStates
//	        rn.Advance(rd)
//	    }
//	}
//
// Ready must be followed by Advance before Ready is called again.
type RawNode struct {
	raft          *raft
	prevSoftState SoftState
	prevHardState HardState
}

// NewRawNode creates a node from the given configuration, restoring its
// state from Config.Storage. A brand-new node has no membership until the
// application calls [RawNode.Bootstrap] or it receives a snapshot from a
// leader (the join flow).
func NewRawNode(cfg *Config) (*RawNode, error) {
	r, err := newRaft(cfg)
	if err != nil {
		return nil, err
	}
	return &RawNode{
		raft:          r,
		prevSoftState: r.softState(),
		prevHardState: r.hardState(),
	}, nil
}

// Peer describes an initial cluster member for Bootstrap. Context is stored
// in the membership entry and is typically the member's address.
type Peer struct {
	ID      NodeID
	Context []byte
}

// Bootstrap initialises a brand-new cluster by appending one membership entry
// per peer at term 1 and marking them committed. Every initial member must be
// bootstrapped with the same peer list so that their logs are identical. The
// entries are delivered through the first Ready for persistence and apply.
func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("raft: bootstrap requires at least one peer")
	}
	r := rn.raft
	last, err := r.raftLog.storage.LastIndex()
	if err != nil {
		return fmt.Errorf("storage last index: %w", err)
	}
	if last != 0 || !r.conf.IsEmpty() || r.Term != 0 {
		return errors.New("raft: cannot bootstrap a node that already has state")
	}

	rn.prevHardState = HardState{}
	r.becomeFollower(1, None)

	ents := make([]Entry, len(peers))
	for i, p := range peers {
		if p.ID == None {
			return errors.New("raft: bootstrap peer id must not be zero")
		}
		cc := ConfChange{Type: ConfChangeAddVoter, NodeID: p.ID, Context: p.Context}
		ents[i] = Entry{Type: EntryConfChange, Term: 1, Index: Index(i + 1), Data: cc.Marshal()}
	}
	r.raftLog.append(ents...)
	// The bootstrap entries are committed by definition. They are not marked
	// applied so that the application observes them through CommittedEntries
	// and can record each peer's context.
	r.raftLog.committed = Index(len(ents))
	for _, p := range peers {
		r.applyConfChange(ConfChange{Type: ConfChangeAddVoter, NodeID: p.ID})
	}
	return nil
}

// Tick advances the logical clock by one tick, driving election and
// heartbeat timeouts.
func (rn *RawNode) Tick() { rn.raft.tick() }

// Campaign starts an election immediately instead of waiting for the
// election timeout.
func (rn *RawNode) Campaign() error {
	r := rn.raft
	if !r.conf.Contains(r.id) {
		return ErrUnknownNode
	}
	if !r.promotable() {
		return ErrLearnerCannotCampaign
	}
	if r.preVote {
		r.hup(campaignPreElection)
	} else {
		r.hup(campaignElection)
	}
	return nil
}

// Propose appends a command to the log and returns the index it was assigned.
// The entry is committed only once a quorum has replicated it; the
// application learns of that through CommittedEntries. If the node loses
// leadership first, a different entry may be committed at the same index, so
// applications must verify the term of the applied entry.
func (rn *RawNode) Propose(data []byte) (Index, error) {
	if err := rn.raft.propose(data); err != nil {
		return 0, err
	}
	return rn.raft.raftLog.lastIndex(), nil
}

// ProposeConfChange appends a membership change and returns its index.
func (rn *RawNode) ProposeConfChange(cc ConfChange) (Index, error) {
	if err := rn.raft.proposeConfChange(cc); err != nil {
		return 0, err
	}
	return rn.raft.raftLog.lastIndex(), nil
}

// ApplyConfChange must be called when the application applies an
// EntryConfChange entry. It returns the resulting membership.
func (rn *RawNode) ApplyConfChange(cc ConfChange) ConfState {
	return rn.raft.applyConfChange(cc)
}

// Step feeds a message received from a peer into the state machine.
func (rn *RawNode) Step(m Message) error {
	if m.Type.IsResponse() && m.From != None {
		if _, ok := rn.raft.prs[m.From]; !ok {
			return ErrStepPeerNotFound
		}
	}
	return rn.raft.Step(m)
}

// ReadIndex requests a linearizable read barrier. Once a quorum confirms the
// leader's authority, a ReadState carrying ctx is delivered in a Ready; the
// application may serve the read after applying up to ReadState.Index. On a
// follower the request is forwarded to the leader; on a candidate, or on a
// follower without a known leader, it is dropped and the application should
// retry after a timeout.
func (rn *RawNode) ReadIndex(ctx []byte) {
	r := rn.raft
	_ = r.Step(Message{Type: MsgReadIndex, From: r.id, To: r.id, Term: r.Term, Context: ctx})
}

// TransferLeader asks the leader to hand over leadership to the given voter.
func (rn *RawNode) TransferLeader(to NodeID) error {
	if rn.raft.state != StateLeader {
		return ErrNotLeader
	}
	if !rn.raft.conf.IsVoter(to) {
		return fmt.Errorf("%w: node %d is not a voter", ErrUnknownNode, to)
	}
	rn.raft.transferLeader(to)
	return nil
}

// ReportUnreachable tells the node that a message to the peer could not be
// delivered.
func (rn *RawNode) ReportUnreachable(id NodeID) { rn.raft.reportUnreachable(id) }

// ReportSnapshot tells the node whether a snapshot transfer succeeded.
func (rn *RawNode) ReportSnapshot(id NodeID, succeeded bool) {
	rn.raft.reportSnapshot(id, succeeded)
}

// HasReady reports whether a call to Ready would return work.
func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if r.softState() != rn.prevSoftState {
		return true
	}
	if hs := r.hardState(); !hs.IsEmpty() && hs != rn.prevHardState {
		return true
	}
	if r.raftLog.hasPendingSnapshot() {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstable) > 0 || r.raftLog.hasNextCommittedEnts() {
		return true
	}
	return len(r.readStates) > 0
}

// Ready returns the pending work. The caller must process it as described in
// the package documentation and then call Advance.
func (rn *RawNode) Ready() Ready {
	r := rn.raft
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextCommittedEnts(),
		Messages:         r.msgs,
		ReadStates:       r.readStates,
	}
	if ss := r.softState(); ss != rn.prevSoftState {
		rd.SoftState = &ss
	}
	if hs := r.hardState(); hs != rn.prevHardState {
		rd.HardState = hs
	}
	if r.raftLog.hasPendingSnapshot() {
		rd.Snapshot = *r.raftLog.pendingSnapshot
	}
	rd.MustSync = len(rd.Entries) > 0 ||
		!rd.Snapshot.IsEmpty() ||
		(!rd.HardState.IsEmpty() &&
			(rd.HardState.Term != rn.prevHardState.Term || rd.HardState.Vote != rn.prevHardState.Vote))

	rn.prevSoftState = r.softState()
	r.msgs = nil
	r.readStates = nil
	return rd
}

// Advance acknowledges that the Ready returned by the last call to Ready has
// been fully processed: its entries and snapshot are stable and its committed
// entries have been applied.
func (rn *RawNode) Advance(rd Ready) {
	r := rn.raft
	if !rd.HardState.IsEmpty() {
		rn.prevHardState = rd.HardState
	}
	if n := len(rd.Entries); n > 0 {
		e := rd.Entries[n-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	if !rd.Snapshot.IsEmpty() {
		r.raftLog.stableSnapTo(rd.Snapshot.Meta.Index)
		r.raftLog.appliedTo(rd.Snapshot.Meta.Index)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.raftLog.appliedTo(rd.CommittedEntries[n-1].Index)
	}
}

// Status returns a snapshot of the node's state for monitoring.
func (rn *RawNode) Status() Status { return rn.raft.status() }

// BasicStatus is the subset of Status that can be produced without
// allocating; it is suitable for hot paths such as tagging proposals with
// the current term.
type BasicStatus struct {
	ID        NodeID
	State     StateType
	Term      Term
	Vote      NodeID
	Leader    NodeID
	Commit    Index
	Applied   Index
	LastIndex Index
}

// BasicStatus returns the node's basic state without the per-follower
// progress map.
func (rn *RawNode) BasicStatus() BasicStatus {
	r := rn.raft
	return BasicStatus{
		ID:        r.id,
		State:     r.state,
		Term:      r.Term,
		Vote:      r.Vote,
		Leader:    r.lead,
		Commit:    r.raftLog.committed,
		Applied:   r.raftLog.applied,
		LastIndex: r.raftLog.lastIndex(),
	}
}

// ConfState returns the current membership.
func (rn *RawNode) ConfState() ConfState { return rn.raft.conf.Clone() }
