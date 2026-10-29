package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sort"
)

// campaignType distinguishes the three reasons a node starts an election.
type campaignType uint8

const (
	// campaignPreElection is the first phase of a Pre-Vote election.
	campaignPreElection campaignType = iota
	// campaignElection is a regular election, with or without a preceding
	// Pre-Vote phase.
	campaignElection
	// campaignTransfer is an election started because the leader asked this
	// node to take over. It skips Pre-Vote and bypasses the lease check.
	campaignTransfer
)

// voteResult is the outcome of tallying votes.
type voteResult uint8

const (
	votePending voteResult = iota
	voteWon
	voteLost
)

type stepFunc func(r *raft, m Message) error

// raft is the deterministic core. It is driven exclusively through RawNode
// and must never be used concurrently.
type raft struct {
	id NodeID

	Term Term
	Vote NodeID

	raftLog *raftLog

	maxMsgSize  uint64
	maxInflight int

	// conf is the current membership; prs holds a Progress for every member,
	// and members lists their ids in ascending order so that iteration is
	// deterministic.
	conf      ConfState
	prs       map[NodeID]*Progress
	members   []NodeID
	isLearner bool

	state StateType
	lead  NodeID

	// votes tallies responses in the current (pre-)election.
	votes map[NodeID]bool

	// msgs accumulates outbound messages until the next Ready.
	msgs []Message

	// readStates accumulates confirmed read requests until the next Ready.
	readStates []ReadState
	readOnly   *readOnly
	// pendingReadIndexMessages holds reads that arrived before the leader
	// committed an entry in its term.
	pendingReadIndexMessages []Message

	// leadTransferee is the target of an in-progress leadership transfer.
	leadTransferee NodeID

	// pendingConfIndex is the index of the latest configuration change in
	// the log. New changes are rejected until it has been applied.
	pendingConfIndex Index

	electionElapsed  int
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int

	tick func()
	step stepFunc

	rand   *rand.Rand
	logger *slog.Logger
}

func newRaft(c *Config) (*raft, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	logger := c.Logger.With("raft_id", c.ID)
	log, err := newLog(c.Storage, logger, c.MaxCommittedSizePerReady)
	if err != nil {
		return nil, err
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("storage initial state: %w", err)
	}
	r := &raft{
		id:               c.ID,
		lead:             None,
		raftLog:          log,
		maxMsgSize:       c.MaxSizePerMsg,
		maxInflight:      c.MaxInflightMsgs,
		prs:              make(map[NodeID]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		readOnly:         newReadOnly(),
		rand:             c.Rand,
		logger:           logger,
	}
	r.switchToConfig(cs)
	if !hs.IsEmpty() {
		if err := r.loadState(hs); err != nil {
			return nil, err
		}
	}
	if c.Applied > 0 {
		if c.Applied > r.raftLog.committed {
			return nil, fmt.Errorf("%w: applied index %d is beyond commit index %d", ErrInvalidConfig, c.Applied, r.raftLog.committed)
		}
		r.raftLog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	r.logger.Info("raft node created",
		"term", r.Term, "vote", r.Vote, "commit", r.raftLog.committed,
		"applied", r.raftLog.applied, "last_index", r.raftLog.lastIndex(),
		"conf", r.conf.String())
	return r, nil
}

// loadState restores the persisted hard state. A commit index below the
// snapshot is ignored (it can happen if a commit-only hard state update was
// not fsynced before a crash), and one beyond the log is corruption.
func (r *raft) loadState(hs HardState) error {
	if hs.Commit > r.raftLog.lastIndex() {
		return fmt.Errorf("%w: persisted commit index %d is beyond last index %d", ErrInvalidConfig, hs.Commit, r.raftLog.lastIndex())
	}
	if hs.Commit > r.raftLog.committed {
		r.raftLog.committed = hs.Commit
	}
	r.Term = hs.Term
	r.Vote = hs.Vote
	return nil
}

func (r *raft) softState() SoftState { return SoftState{Leader: r.lead, State: r.state} }

func (r *raft) hardState() HardState {
	return HardState{Term: r.Term, Vote: r.Vote, Commit: r.raftLog.committed}
}

func (r *raft) quorum() int { return len(r.conf.Voters)/2 + 1 }

// promotable reports whether the node may become leader: it must be a voter
// and must not be in the middle of installing a snapshot.
func (r *raft) promotable() bool {
	pr, ok := r.prs[r.id]
	return ok && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

// send queues a message for delivery in the next Ready. Vote messages carry
// an explicit term (Pre-Vote uses a future term); everything else is stamped
// with the current term.
func (r *raft) send(m Message) {
	if m.From == None {
		m.From = r.id
	}
	switch m.Type {
	case MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp:
		if m.Term == 0 {
			panic(fmt.Sprintf("raft: term must be set when sending %s", m.Type))
		}
	default:
		if m.Term != 0 {
			panic(fmt.Sprintf("raft: term must not be set when sending %s (was %d)", m.Type, m.Term))
		}
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an AppendEntries (or a snapshot) to the given peer,
// probing with an empty message if there is nothing new.
func (r *raft) sendAppend(to NodeID) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends entries to the peer if it is not paused. With
// sendIfEmpty false, nothing is sent when there are no entries to send. It
// returns true if a message was queued.
func (r *raft) maybeSendAppend(to NodeID, sendIfEmpty bool) bool {
	pr := r.prs[to]
	if pr == nil || pr.isPaused() {
		return false
	}

	prevIndex := pr.Next - 1
	prevTerm, errTerm := r.raftLog.term(prevIndex)
	ents, errEnts := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errTerm != nil || errEnts != nil {
		// The follower needs entries that have been compacted: send a
		// snapshot instead.
		if !pr.RecentActive {
			r.logger.Debug("not sending snapshot to inactive peer", "to", to)
			return false
		}
		snap, err := r.raftLog.snapshot()
		if err != nil {
			if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
				r.logger.Debug("snapshot temporarily unavailable", "to", to)
				return false
			}
			panic(fmt.Sprintf("raft: snapshot: %v", err))
		}
		if snap.IsEmpty() {
			panic("raft: storage returned an empty snapshot while entries are compacted")
		}
		pr.becomeSnapshot(snap.Meta.Index)
		r.logger.Info("sending snapshot",
			"to", to, "snapshot_index", snap.Meta.Index, "snapshot_term", snap.Meta.Term,
			"first_index", r.raftLog.firstIndex(), "commit", r.raftLog.committed, "progress", pr.String())
		r.send(Message{To: to, Type: MsgSnap, Snapshot: &snap})
		return true
	}

	m := Message{
		To:           to,
		Type:         MsgApp,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		Commit:       r.raftLog.committed,
	}
	if n := len(ents); n > 0 {
		pr.sentEntries(ents[n-1].Index)
	}
	r.send(m)
	return true
}

func (r *raft) sendHeartbeat(to NodeID, ctx []byte) {
	// The commit index sent with a heartbeat is capped at what the follower
	// is known to have, because a follower moves its commit index without
	// checking that its log matches.
	commit := min(r.prs[to].Match, r.raftLog.committed)
	r.send(Message{To: to, Type: MsgHeartbeat, Commit: commit, Context: ctx})
}

func (r *raft) bcastAppend() {
	for _, id := range r.members {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *raft) bcastHeartbeat() {
	r.bcastHeartbeatWithCtx(r.readOnly.lastPendingRequestCtx())
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for _, id := range r.members {
		if id != r.id {
			r.sendHeartbeat(id, ctx)
		}
	}
}

// appendEntry stamps the entries with the current term and consecutive
// indexes, appends them, and updates the leader's own progress.
func (r *raft) appendEntry(es ...Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + Index(i)
	}
	li = r.raftLog.append(es...)
	r.prs[r.id].maybeUpdate(li)
	// A single-voter cluster commits immediately; otherwise the caller's
	// broadcast will collect acknowledgements.
	r.maybeCommit()
}

// quorumMatchIndex returns the highest index replicated on a majority of
// voters.
func (r *raft) quorumMatchIndex() Index {
	matches := make([]Index, 0, len(r.conf.Voters))
	for _, id := range r.conf.Voters {
		if pr, ok := r.prs[id]; ok {
			matches = append(matches, pr.Match)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	return matches[r.quorum()-1]
}

// maybeCommit advances the commit index if a majority has replicated an
// entry of the current term (the Figure 8 restriction: entries from earlier
// terms are committed only indirectly).
func (r *raft) maybeCommit() bool {
	if len(r.conf.Voters) == 0 {
		return false
	}
	mci := r.quorumMatchIndex()
	if mci <= r.raftLog.committed {
		return false
	}
	if r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(mci)) != r.Term {
		return false
	}
	r.raftLog.commitTo(mci)
	return true
}

// committedEntryInCurrentTerm reports whether the leader has committed at
// least one entry of its own term. Until it has, the commit index may not
// cover every committed entry (Figure 8), so reads and membership changes
// must wait.
func (r *raft) committedEntryInCurrentTerm() bool {
	return r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(r.raftLog.committed)) == r.Term
}

// reset prepares the node for a new term.
func (r *raft) reset(term Term) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[NodeID]bool)
	last := r.raftLog.lastIndex()
	for id, pr := range r.prs {
		*pr = Progress{
			Next:      last + 1,
			Inflights: newInflights(r.maxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = last
		}
	}

	r.pendingConfIndex = 0
	r.readOnly = newReadOnly()
	r.pendingReadIndexMessages = nil
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + r.rand.IntN(r.electionTimeout)
}

func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) becomeFollower(term Term, lead NodeID) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Info("became follower", "term", r.Term, "leader", lead)
}

func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition [leader -> pre-candidate]")
	}
	// Pre-Vote does not change the term or the vote; it only changes the
	// step function, the tally, and forgets the leader.
	r.step = stepCandidate
	r.votes = make(map[NodeID]bool)
	r.tick = r.tickElection
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Info("became pre-candidate", "term", r.Term)
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Info("became candidate", "term", r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("raft: invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader

	// The leader's own log trivially matches; reset() already set Match.
	pr := r.prs[r.id]
	pr.becomeReplicate()
	pr.RecentActive = true

	// Conservatively assume a configuration change may be pending anywhere
	// in the tail of the log: proposals of new changes wait until everything
	// currently in the log has been applied.
	r.pendingConfIndex = r.raftLog.lastIndex()

	// Append an empty entry so that entries from earlier terms can be
	// committed (Figure 8) and so that ReadIndex can be served.
	r.appendEntry(Entry{Type: EntryNormal})
	r.logger.Info("became leader", "term", r.Term, "last_index", r.raftLog.lastIndex())
}

// hup starts an election of the given kind, unless the node is not allowed
// to campaign.
func (r *raft) hup(t campaignType) {
	if r.state == StateLeader {
		r.logger.Debug("ignoring election request: already leader")
		return
	}
	if !r.promotable() {
		r.logger.Warn("cannot campaign: node is not a promotable member", "term", r.Term)
		return
	}
	if r.hasUnappliedConfChanges() {
		r.logger.Warn("cannot campaign: configuration changes are waiting to be applied", "term", r.Term)
		return
	}
	r.logger.Info("starting election", "term", r.Term, "kind", t)
	r.campaign(t)
}

// hasUnappliedConfChanges reports whether a committed configuration change
// has not been applied yet. Campaigning in that state could let a node that
// is about to be removed win an election.
func (r *raft) hasUnappliedConfChanges() bool {
	lo, hi := r.raftLog.applied+1, r.raftLog.committed+1
	if lo >= hi {
		return false
	}
	ents, err := r.raftLog.slice(lo, hi, ^uint64(0))
	if err != nil {
		panic(fmt.Sprintf("raft: unexpected error scanning committed entries: %v", err))
	}
	for _, e := range ents {
		if e.Type == EntryConfChange {
			return true
		}
	}
	return false
}

func (r *raft) campaign(t campaignType) {
	if !r.promotable() {
		r.logger.Warn("cannot campaign: node is not promotable")
		return
	}
	var term Term
	var voteMsg MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = MsgPreVote
		// Pre-Vote asks about the term we would campaign in.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = MsgVote
		term = r.Term
	}

	// Vote for ourselves. In a single-voter cluster that already wins.
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == voteWon {
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}

	for _, id := range r.conf.Voters {
		if id == r.id {
			continue
		}
		r.logger.Info("sending vote request",
			"type", voteMsg, "to", id, "term", term,
			"last_log_index", r.raftLog.lastIndex(), "last_log_term", r.raftLog.lastTerm())
		r.send(Message{
			To:           id,
			Term:         term,
			Type:         voteMsg,
			LastLogIndex: r.raftLog.lastIndex(),
			LastLogTerm:  r.raftLog.lastTerm(),
			Transfer:     t == campaignTransfer,
		})
	}
}

// poll records a vote and tallies the result over the current voters.
func (r *raft) poll(id NodeID, t MessageType, granted bool) (int, int, voteResult) {
	if granted {
		r.logger.Info("received vote", "type", t, "from", id, "term", r.Term)
	} else {
		r.logger.Info("received vote rejection", "type", t, "from", id, "term", r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = granted
	}
	var yes, no int
	for _, v := range r.conf.Voters {
		g, ok := r.votes[v]
		if !ok {
			continue
		}
		if g {
			yes++
		} else {
			no++
		}
	}
	q := r.quorum()
	switch {
	case yes >= q:
		return yes, no, voteWon
	case no > len(r.conf.Voters)-q:
		return yes, no, voteLost
	default:
		return yes, no, votePending
	}
}

func voteRespMsgType(t MessageType) MessageType {
	switch t {
	case MsgVote:
		return MsgVoteResp
	case MsgPreVote:
		return MsgPreVoteResp
	default:
		panic(fmt.Sprintf("raft: %s is not a vote message", t))
	}
}

// Step advances the state machine with a message from a peer (or a local
// MsgReadIndex). It performs the term handling common to all states before
// dispatching to the state-specific step function.
func (r *raft) Step(m Message) error {
	switch {
	case m.Term == 0:
		return fmt.Errorf("%w: %s from %s has no term", ErrStepLocalMsg, m.Type, m.From)

	case m.Term > r.Term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			// A follower that is in contact with a leader ignores vote
			// requests, unless the vote is part of a leadership transfer.
			// This stops a partitioned node from disrupting a working
			// cluster once it reconnects.
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !m.Transfer && inLease {
				r.logger.Info("ignoring vote request: still within leader lease",
					"type", m.Type, "from", m.From, "msg_term", m.Term, "term", r.Term,
					"leader", r.lead, "remaining_ticks", r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// Never change the term in response to a Pre-Vote.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// Pre-Vote requests carry a future term. If granted, the term is
			// incremented once a quorum is reached; if rejected, the term
			// comes from the rejecting node and is handled below.
		default:
			r.logger.Info("received message with higher term; becoming follower",
				"type", m.Type, "from", m.From, "msg_term", m.Term, "term", r.Term)
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		switch {
		case (r.checkQuorum || r.preVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp):
			// A leader from an older term is still alive and does not know
			// that it has been superseded. With CheckQuorum its own term will
			// not advance through vote requests (we ignore them during the
			// lease), so answer with our term to make it step down.
			r.send(Message{To: m.From, Type: MsgAppResp})
		case m.Type == MsgPreVote:
			// Answer with our term so that the pre-candidate learns it.
			r.send(Message{To: m.From, Term: r.Term, Type: MsgPreVoteResp, Reject: true})
		default:
			r.logger.Debug("ignoring message with lower term",
				"type", m.Type, "from", m.From, "msg_term", m.Term, "term", r.Term)
		}
		return nil
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		// We may vote if we already voted for this candidate, or if we have
		// neither voted nor heard from a leader in this term, or (Pre-Vote
		// only) if the request is for a future term; and in all cases only
		// if the candidate's log is at least as complete as ours.
		canVote := r.Vote == m.From ||
			(r.Vote == None && r.lead == None) ||
			(m.Type == MsgPreVote && m.Term > r.Term)
		if canVote && r.raftLog.isUpToDate(m.LastLogIndex, m.LastLogTerm) {
			r.logger.Info("granting vote",
				"type", m.Type, "to", m.From, "msg_term", m.Term, "term", r.Term,
				"candidate_last_index", m.LastLogIndex, "candidate_last_term", m.LastLogTerm)
			// Respond with the request's term so that a pre-candidate can
			// tally the response against the future term it asked about.
			r.send(Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == MsgVote {
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Info("rejecting vote",
				"type", m.Type, "to", m.From, "msg_term", m.Term, "term", r.Term, "vote", r.Vote, "leader", r.lead,
				"candidate_last_index", m.LastLogIndex, "candidate_last_term", m.LastLogTerm,
				"last_index", r.raftLog.lastIndex(), "last_term", r.raftLog.lastTerm())
			r.send(Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}
		return nil
	default:
		return r.step(r, m)
	}
}

func stepLeader(r *raft, m Message) error {
	if m.Type == MsgReadIndex {
		r.handleReadIndex(m)
		return nil
	}

	pr := r.prs[m.From]
	if pr == nil {
		r.logger.Debug("ignoring message from unknown peer", "type", m.Type, "from", m.From)
		return nil
	}

	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			r.logger.Debug("received append rejection",
				"from", m.From, "rejected_index", m.PrevLogIndex, "hint", m.RejectHint,
				"conflict_term", m.ConflictTerm, "conflict_index", m.ConflictIndex, "progress", pr.String())
			next := r.raftLog.nextIndexAfterReject(m.PrevLogIndex, m.ConflictTerm, m.ConflictIndex)
			next = min(next, m.RejectHint+1)
			if pr.maybeDecrTo(m.PrevLogIndex, next) {
				if pr.State == ProgressReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
			return nil
		}

		wasPaused := pr.isPaused()
		if !pr.maybeUpdate(m.MatchIndex) {
			return nil
		}
		switch {
		case pr.State == ProgressProbe:
			pr.becomeReplicate()
		case pr.State == ProgressSnapshot && pr.Match >= pr.PendingSnapshot:
			// The follower acknowledged the snapshot; resume replication.
			r.logger.Info("snapshot acknowledged; resuming replication", "to", m.From, "match", pr.Match)
			pr.becomeProbe()
			pr.becomeReplicate()
		case pr.State == ProgressReplicate:
			pr.Inflights.freeLE(m.MatchIndex)
		}

		if r.maybeCommit() {
			r.releasePendingReadIndexMessages()
			r.bcastAppend()
		} else if wasPaused {
			// The follower may be missing the latest commit index.
			r.sendAppend(m.From)
		}
		// Stream any remaining entries.
		for r.maybeSendAppend(m.From, false) {
		}
		if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
			r.logger.Info("transfer target caught up; sending timeout-now", "to", m.From)
			r.sendTimeoutNow(m.From)
		}

	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.MsgAppFlowPaused = false
		// If the in-flight window is full, free one slot so that replication
		// can recover if the acknowledgements that would have freed it were
		// lost.
		if pr.State == ProgressReplicate && pr.Inflights.full() {
			pr.Inflights.freeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}
		if len(m.Context) == 0 {
			return nil
		}
		acks := r.readOnly.recvAck(m.From, m.Context)
		if r.countVoterAcks(acks) < r.quorum() {
			return nil
		}
		for _, rs := range r.readOnly.advance(m.Context) {
			r.respondToReadIndex(rs.req, rs.index)
		}

	default:
		r.logger.Debug("ignoring message as leader", "type", m.Type, "from", m.From)
	}
	return nil
}

func (r *raft) countVoterAcks(acks map[NodeID]struct{}) int {
	n := 0
	for id := range acks {
		if r.conf.IsVoter(id) {
			n++
		}
	}
	return n
}

func stepCandidate(r *raft, m Message) error {
	// Only one kind of vote response is relevant: Pre-Vote responses while
	// pre-campaigning, real vote responses while campaigning.
	myVoteResp := MsgVoteResp
	if r.state == StatePreCandidate {
		myVoteResp = MsgPreVoteResp
	}

	switch m.Type {
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteResp:
		yes, no, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Info("vote tally", "type", m.Type, "granted", yes, "rejected", no, "quorum", r.quorum())
		switch res {
		case voteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case voteLost:
			// A rejected Pre-Vote response may carry a future term; staying
			// in the current term is correct in both cases.
			r.becomeFollower(r.Term, None)
		case votePending:
		}
	case MsgTimeoutNow:
		r.logger.Debug("ignoring timeout-now as candidate", "from", m.From)
	default:
		r.logger.Debug("ignoring message as candidate", "type", m.Type, "from", m.From)
	}
	return nil
}

func stepFollower(r *raft, m Message) error {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTimeoutNow:
		r.logger.Info("received timeout-now; starting transfer election", "from", m.From, "term", r.Term)
		// A transfer election skips Pre-Vote: the leader asked for it, so
		// there is no risk of disrupting a healthy cluster.
		r.hup(campaignTransfer)
	case MsgReadIndex:
		if r.lead == None {
			r.logger.Info("dropping read request: no leader known", "term", r.Term)
			return nil
		}
		// Forward to the leader; send stamps the current term.
		m.To = r.lead
		m.From = r.id
		m.Term = 0
		r.send(m)
	case MsgReadIndexResp:
		r.readStates = append(r.readStates, ReadState{Index: m.ReadIndex, Context: m.Context})
	default:
		r.logger.Debug("ignoring message as follower", "type", m.Type, "from", m.From)
	}
	return nil
}

// validEntries checks that the entries of a MsgApp are contiguous from
// PrevLogIndex+1 with non-decreasing terms. A correct leader always sends
// such entries; a buggy or hostile peer must not be able to crash the node.
func validEntries(m Message) bool {
	prevTerm := m.PrevLogTerm
	for i, e := range m.Entries {
		if e.Index != m.PrevLogIndex+1+Index(i) || e.Term < prevTerm || e.Term == 0 {
			return false
		}
		if e.Type != EntryNormal && e.Type != EntryConfChange {
			return false
		}
		prevTerm = e.Term
	}
	return true
}

func (r *raft) handleAppendEntries(m Message) {
	if !validEntries(m) {
		r.logger.Error("dropping malformed append; the sender is buggy or hostile",
			"from", m.From, "prev_index", m.PrevLogIndex, "entries", len(m.Entries))
		return
	}
	if m.PrevLogIndex < r.raftLog.committed {
		// Everything up to the commit index is final; tell the leader where
		// we are instead of re-checking entries it already knows we have.
		r.send(Message{To: m.From, Type: MsgAppResp, MatchIndex: r.raftLog.committed})
		return
	}
	if lastNew, ok := r.raftLog.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.Commit, m.Entries); ok {
		r.send(Message{To: m.From, Type: MsgAppResp, MatchIndex: lastNew})
		return
	}
	conflictTerm, conflictIndex := r.raftLog.conflictHint(m.PrevLogIndex)
	r.logger.Debug("rejecting append: log mismatch",
		"from", m.From, "prev_index", m.PrevLogIndex, "prev_term", m.PrevLogTerm,
		"local_term", r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(m.PrevLogIndex)),
		"last_index", r.raftLog.lastIndex(), "conflict_term", conflictTerm, "conflict_index", conflictIndex)
	r.send(Message{
		To:            m.From,
		Type:          MsgAppResp,
		Reject:        true,
		PrevLogIndex:  m.PrevLogIndex,
		RejectHint:    r.raftLog.lastIndex(),
		ConflictTerm:  conflictTerm,
		ConflictIndex: conflictIndex,
	})
}

func (r *raft) handleHeartbeat(m Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		r.logger.Warn("ignoring snapshot message without a snapshot", "from", m.From)
		r.send(Message{To: m.From, Type: MsgAppResp, MatchIndex: r.raftLog.committed})
		return
	}
	s := *m.Snapshot
	if r.restore(s) {
		r.logger.Info("restored snapshot",
			"from", m.From, "snapshot_index", s.Meta.Index, "snapshot_term", s.Meta.Term, "commit", r.raftLog.committed)
		r.send(Message{To: m.From, Type: MsgAppResp, MatchIndex: r.raftLog.lastIndex()})
	} else {
		r.logger.Info("ignored snapshot",
			"from", m.From, "snapshot_index", s.Meta.Index, "snapshot_term", s.Meta.Term, "commit", r.raftLog.committed)
		r.send(Message{To: m.From, Type: MsgAppResp, MatchIndex: r.raftLog.committed})
	}
}

// restore installs a snapshot received from the leader. It returns true if
// the log was reset to the snapshot, false if the snapshot was stale or the
// log already contains its last entry.
func (r *raft) restore(s Snapshot) bool {
	if s.Meta.Index <= r.raftLog.committed {
		return false
	}
	if r.state != StateFollower {
		// Should be unreachable: snapshots are handled only by followers.
		// Stepping down is the safe reaction to an impossible state.
		r.logger.Error("attempted to restore a snapshot while not a follower; stepping down", "state", r.state)
		r.becomeFollower(r.Term+1, None)
		return false
	}
	if !s.Meta.ConfState.Contains(r.id) {
		// A node that joins an existing cluster normally receives a snapshot
		// taken before its own membership entry: the entries that follow the
		// snapshot add it. Restoring is therefore the right thing to do; the
		// node stays unpromotable until it applies that entry.
		r.logger.Info("restoring snapshot whose membership does not include this node yet",
			"snapshot_index", s.Meta.Index, "conf", s.Meta.ConfState.String())
	}
	if r.raftLog.matchTerm(s.Meta.Index, s.Meta.Term) {
		// The log already contains the snapshot's last entry; just move the
		// commit index forward.
		r.logger.Info("fast-forwarding commit to snapshot index",
			"snapshot_index", s.Meta.Index, "snapshot_term", s.Meta.Term, "commit", r.raftLog.committed)
		r.raftLog.commitTo(s.Meta.Index)
		return false
	}
	r.raftLog.restore(s)
	r.switchToConfig(s.Meta.ConfState)
	return true
}

// switchToConfig installs a new membership and reconciles the progress map.
// On the leader it also re-evaluates the commit index (the quorum may have
// changed), starts replicating to new members, and steps down if the leader
// itself was removed or demoted.
func (r *raft) switchToConfig(cs ConfState) ConfState {
	cs = cs.Clone()
	r.conf = cs
	next := r.raftLog.lastIndex() + 1

	seen := make(map[NodeID]bool, len(cs.Voters)+len(cs.Learners))
	for _, id := range cs.Voters {
		seen[id] = true
		if pr, ok := r.prs[id]; ok {
			pr.IsLearner = false
		} else {
			r.prs[id] = newProgress(next, r.maxInflight, false)
		}
	}
	for _, id := range cs.Learners {
		seen[id] = true
		if pr, ok := r.prs[id]; ok {
			pr.IsLearner = true
		} else {
			r.prs[id] = newProgress(next, r.maxInflight, true)
		}
	}
	for id := range r.prs {
		if !seen[id] {
			delete(r.prs, id)
		}
	}
	r.members = make([]NodeID, 0, len(r.prs))
	for id := range r.prs {
		r.members = append(r.members, id)
	}
	slices.Sort(r.members)

	pr, ok := r.prs[r.id]
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		r.logger.Info("leader removed or demoted by configuration change; stepping down", "conf", cs.String())
		r.becomeFollower(r.Term, None)
		return cs
	}
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		r.releasePendingReadIndexMessages()
		r.bcastAppend()
	} else {
		for _, id := range r.members {
			if id != r.id {
				r.maybeSendAppend(id, false)
			}
		}
	}
	r.releaseReadsAfterConfChange()
	if r.leadTransferee != None && !cs.IsVoter(r.leadTransferee) {
		r.abortLeaderTransfer()
	}
	return cs
}

func (r *raft) applyConfChange(cc ConfChange) ConfState {
	return r.switchToConfig(r.conf.apply(cc))
}

// validateChange rejects configuration changes that would be no-ops or would
// leave the cluster without voters.
func (cs ConfState) validateChange(cc ConfChange) error {
	switch cc.Type {
	case ConfChangeAddVoter:
		if cs.IsVoter(cc.NodeID) {
			return fmt.Errorf("%w: node %d is already a voter", ErrInvalidConfChange, cc.NodeID)
		}
	case ConfChangeAddLearner:
		if cs.Contains(cc.NodeID) {
			return fmt.Errorf("%w: node %d is already a member", ErrInvalidConfChange, cc.NodeID)
		}
	case ConfChangeRemoveNode:
		if !cs.Contains(cc.NodeID) {
			return fmt.Errorf("%w: node %d is not a member", ErrInvalidConfChange, cc.NodeID)
		}
		if cs.IsVoter(cc.NodeID) && len(cs.Voters) == 1 {
			return fmt.Errorf("%w: cannot remove the last voter", ErrInvalidConfChange)
		}
	default:
		return fmt.Errorf("%w: unknown change type %d", ErrInvalidConfChange, cc.Type)
	}
	if cc.NodeID == None {
		return fmt.Errorf("%w: node id must not be zero", ErrInvalidConfChange)
	}
	return nil
}

func (r *raft) propose(data []byte) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.leadTransferee != None {
		r.logger.Debug("dropping proposal: leadership transfer in progress", "transferee", r.leadTransferee)
		return ErrProposalDropped
	}
	r.appendEntry(Entry{Type: EntryNormal, Data: data})
	r.bcastAppend()
	return nil
}

func (r *raft) proposeConfChange(cc ConfChange) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.leadTransferee != None {
		return ErrProposalDropped
	}
	if r.pendingConfIndex > r.raftLog.applied {
		return fmt.Errorf("%w: change at index %d is not applied yet (applied %d)",
			ErrConfChangePending, r.pendingConfIndex, r.raftLog.applied)
	}
	if !r.committedEntryInCurrentTerm() {
		return ErrLeaderTermNotCommitted
	}
	if err := r.conf.validateChange(cc); err != nil {
		return err
	}
	r.appendEntry(Entry{Type: EntryConfChange, Data: cc.Marshal()})
	r.pendingConfIndex = r.raftLog.lastIndex()
	r.logger.Info("proposed configuration change", "change", cc.String(), "index", r.pendingConfIndex)
	r.bcastAppend()
	return nil
}

// handleReadIndex processes a read request on the leader.
func (r *raft) handleReadIndex(m Message) {
	if len(r.conf.Voters) == 1 && r.conf.IsVoter(r.id) {
		// A single voter is always a quorum of itself.
		r.respondToReadIndex(m, r.raftLog.committed)
		return
	}
	if !r.committedEntryInCurrentTerm() {
		// The commit index may not cover every committed entry until an
		// entry of this term is committed; hold the request until then.
		r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m)
		return
	}
	key := r.readOnly.addRequest(r.raftLog.committed, m)
	r.readOnly.recvAck(r.id, key)
	r.bcastHeartbeatWithCtx(key)
}

// releaseReadsAfterConfChange re-evaluates pending reads against the new
// voter set: a change that removes voters can turn acknowledgements already
// collected into a quorum, and a single remaining voter needs no
// acknowledgement at all. Without this, such reads would wait for a
// heartbeat response that never comes.
func (r *raft) releaseReadsAfterConfChange() {
	var lastSatisfied []byte
	for _, key := range r.readOnly.pendingKeys() {
		if r.countVoterAcks(r.readOnly.recvAck(r.id, []byte(key))) >= r.quorum() {
			lastSatisfied = []byte(key)
		}
	}
	if lastSatisfied == nil {
		return
	}
	for _, rs := range r.readOnly.advance(lastSatisfied) {
		r.respondToReadIndex(rs.req, rs.index)
	}
}

func (r *raft) respondToReadIndex(req Message, index Index) {
	if req.From == None || req.From == r.id {
		r.readStates = append(r.readStates, ReadState{Index: index, Context: req.Context})
		return
	}
	r.send(Message{To: req.From, Type: MsgReadIndexResp, ReadIndex: index, Context: req.Context})
}

func (r *raft) releasePendingReadIndexMessages() {
	if len(r.pendingReadIndexMessages) == 0 || !r.committedEntryInCurrentTerm() {
		return
	}
	msgs := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil
	for _, m := range msgs {
		r.handleReadIndex(m)
	}
}

func (r *raft) transferLeader(to NodeID) {
	if to == None || to == r.id {
		return
	}
	if r.leadTransferee != None {
		if r.leadTransferee == to {
			r.logger.Debug("leadership transfer already in progress", "to", to)
			return
		}
		r.abortLeaderTransfer()
		r.logger.Info("aborted previous leadership transfer", "previous", r.leadTransferee, "to", to)
	}
	pr := r.prs[to]
	if pr == nil || pr.IsLearner {
		r.logger.Warn("ignoring leadership transfer to a node that is not a voter", "to", to)
		return
	}
	r.logger.Info("starting leadership transfer", "to", to, "term", r.Term)
	// The transfer must finish within one election timeout or it is
	// abandoned (see tickHeartbeat).
	r.electionElapsed = 0
	r.leadTransferee = to
	if pr.Match == r.raftLog.lastIndex() {
		r.sendTimeoutNow(to)
	} else {
		r.sendAppend(to)
	}
}

func (r *raft) abortLeaderTransfer() { r.leadTransferee = None }

func (r *raft) sendTimeoutNow(to NodeID) {
	r.send(Message{To: to, Type: MsgTimeoutNow})
}

// reportUnreachable is called by the application when a message to a peer
// could not be delivered; optimistic replication to it is suspended.
func (r *raft) reportUnreachable(id NodeID) {
	pr := r.prs[id]
	if pr == nil || r.state != StateLeader {
		return
	}
	if pr.State == ProgressReplicate {
		pr.becomeProbe()
		r.logger.Debug("peer unreachable; switching to probe", "to", id)
	}
}

// reportSnapshot is called by the application with the result of a snapshot
// transfer.
func (r *raft) reportSnapshot(id NodeID, succeeded bool) {
	pr := r.prs[id]
	if pr == nil || r.state != StateLeader || pr.State != ProgressSnapshot {
		return
	}
	if succeeded {
		r.logger.Info("snapshot transfer succeeded", "to", id, "snapshot_index", pr.PendingSnapshot)
		pr.becomeProbe()
	} else {
		r.logger.Warn("snapshot transfer failed", "to", id, "snapshot_index", pr.PendingSnapshot)
		pr.PendingSnapshot = 0
		pr.becomeProbe()
	}
	// After a success, wait for the follower's acknowledgement before sending
	// more; after a failure, wait for a heartbeat response before retrying.
	pr.MsgAppFlowPaused = true
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		if r.preVote {
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum && !r.checkQuorumActive() {
			r.logger.Warn("stepping down: lost contact with a quorum", "term", r.Term)
			r.becomeFollower(r.Term, None)
			return
		}
		if r.state == StateLeader && r.leadTransferee != None {
			r.logger.Warn("aborting leadership transfer: timed out", "to", r.leadTransferee)
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
		// A transfer target that is up to date was asked to campaign; repeat
		// the request every heartbeat in case it was lost, since proposals
		// are refused until the transfer completes or times out.
		if pr := r.prs[r.leadTransferee]; pr != nil && pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(r.leadTransferee)
		}
	}
}

// checkQuorumActive reports whether a quorum of voters responded during the
// last election timeout, and resets the activity flags for the next period.
func (r *raft) checkQuorumActive() bool {
	active := 0
	for _, id := range r.members {
		pr := r.prs[id]
		if id == r.id {
			if !pr.IsLearner {
				active++
			}
			continue
		}
		if pr.RecentActive && !pr.IsLearner {
			active++
		}
		pr.RecentActive = false
	}
	return active >= r.quorum()
}

func (r *raft) status() Status {
	s := Status{
		ID:        r.id,
		State:     r.state,
		Term:      r.Term,
		Vote:      r.Vote,
		Leader:    r.lead,
		Commit:    r.raftLog.committed,
		Applied:   r.raftLog.applied,
		LastIndex: r.raftLog.lastIndex(),
		ConfState: r.conf.Clone(),
	}
	if r.state == StateLeader {
		s.Progress = make(map[NodeID]ProgressStatus, len(r.prs))
		for id, pr := range r.prs {
			s.Progress[id] = pr.status()
		}
	}
	return s
}

// String implements fmt.Stringer for campaignType.
func (t campaignType) String() string {
	switch t {
	case campaignPreElection:
		return "pre-election"
	case campaignElection:
		return "election"
	case campaignTransfer:
		return "transfer"
	default:
		return "unknown"
	}
}
