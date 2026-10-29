package raft

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
)

// This file contains a deterministic cluster simulator. It drives RawNodes
// through an adversarial network from a seeded random source, so every run
// is reproducible from its seed. It models message delay, loss, duplication
// and reordering; symmetric partitions and one-way link failures; crashes
// that lose unsynced state and restarts from persisted storage; log
// compaction through snapshots; leadership transfers; and membership changes.
//
// After every tick it checks the safety properties of Raft:
//
//   - Election Safety: at most one leader per term.
//   - Leader Completeness: a newly elected leader holds every entry that any
//     node has already applied.
//   - Log Matching: logs that agree on an entry's term agree on everything
//     before it.
//   - State Machine Safety: no two nodes apply different entries at the same
//     index, and every node applies entries in order.
//   - Monotonicity: terms and commit indexes never go backwards, including
//     across restarts.
//   - Linearizable reads: a confirmed ReadIndex is at least as large as the
//     highest index any node had applied when the read was requested.
//
// Once the chaos phase ends, the network is healed and liveness is checked:
// a leader emerges, every member converges on the same applied state, and
// new proposals commit.
//
// Run a single seed with: go test ./raft -run TestSimulation -sim.seed=N -v

var (
	simSeeds = flag.Int("sim.seeds", 30, "number of random seeds per simulation scenario")
	simSeed  = flag.Uint64("sim.seed", 0, "run only this seed (0 means all)")
	simTicks = flag.Int("sim.ticks", 0, "override the number of chaos ticks per run")
)

type simConfig struct {
	name            string
	voters          int     // initial voters are ids 1..voters
	spares          int     // extra ids that start outside the cluster
	ticks           int     // chaos-phase ticks
	dropRate        float64 // probability that a message is lost
	dupRate         float64 // probability that a message is duplicated
	maxDelay        int     // maximum delivery delay in ticks (minimum is 1)
	partitionEvery  int     // mean ticks between partition changes (0 disables)
	linkFlapEvery   int     // mean ticks between one-way link failures
	crashEvery      int     // mean ticks between crashes (0 disables)
	maxDowntime     int     // maximum ticks a crashed node stays down
	proposeRate     float64 // probability per tick of a proposal
	readRate        float64 // probability per tick of a ReadIndex request
	transferEvery   int     // mean ticks between leadership transfers (0 disables)
	snapshotEvery   Index   // entries between compactions (0 disables)
	membershipEvery int     // mean ticks between membership changes (0 disables)
	convergeTicks   int     // ticks allowed for liveness after the chaos phase
}

func (c simConfig) electionTick() int { return 10 }

type simMsg struct {
	deliverAt int
	msg       Message
}

type simNode struct {
	id      NodeID
	rn      *RawNode
	storage *MemoryStorage
	alive   bool
	upSince int
	downAt  int

	fsm          []Entry // applied entries, the state machine
	conf         ConfState
	appliedIndex Index
	syncedHS     HardState // the last hard state that reached "disk"
	lastTerm     Term
	lastCommit   Index

	pendingReads map[string]Index // read context -> global applied index at request
	servedReads  map[string]bool  // contexts already confirmed (duplicates are legal)
}

type simStats struct {
	proposals, proposalsAccepted int
	applied                      int
	readsRequested, readsServed  int
	elections                    int
	snapshotsSent                int
	snapshotsTaken               int
	crashes                      int
	partitions                   int
	confChanges                  int
	transfers                    int
	messages, dropped            int
}

type sim struct {
	t    *testing.T
	cfg  simConfig
	seed uint64
	rng  *rand.Rand

	ids   []NodeID
	nodes map[NodeID]*simNode
	now   int
	chaos bool

	queue          []simMsg
	partition      map[NodeID]int
	partitionUntil int
	linkDown       map[[2]NodeID]int

	leaders       map[Term]NodeID
	committed     map[Index]Entry
	appliedData   map[string]bool
	maxAppliedAny Index

	nextReadCtx int
	trace       []string
	stats       simStats
	failed      bool
}

func newSim(t *testing.T, cfg simConfig, seed uint64) *sim {
	t.Helper()
	s := &sim{
		t:           t,
		cfg:         cfg,
		seed:        seed,
		rng:         rand.New(rand.NewPCG(seed, 0xC0FFEE)),
		nodes:       make(map[NodeID]*simNode),
		partition:   make(map[NodeID]int),
		linkDown:    make(map[[2]NodeID]int),
		leaders:     make(map[Term]NodeID),
		committed:   make(map[Index]Entry),
		appliedData: make(map[string]bool),
	}
	peers := make([]Peer, 0, cfg.voters)
	for i := 1; i <= cfg.voters; i++ {
		peers = append(peers, Peer{ID: NodeID(i), Context: fmt.Appendf(nil, "addr-%d", i)})
	}
	for i := 1; i <= cfg.voters+cfg.spares; i++ {
		id := NodeID(i)
		s.ids = append(s.ids, id)
		n := &simNode{id: id, storage: NewMemoryStorage(), alive: true, pendingReads: make(map[string]Index), servedReads: make(map[string]bool)}
		n.rn = s.newRawNode(n, 0)
		if i <= cfg.voters {
			if err := n.rn.Bootstrap(peers); err != nil {
				t.Fatalf("bootstrap %d: %v", id, err)
			}
		}
		s.nodes[id] = n
	}
	return s
}

func (s *sim) newRawNode(n *simNode, applied Index) *RawNode {
	cfg := &Config{
		ID:              n.id,
		ElectionTick:    s.cfg.electionTick(),
		HeartbeatTick:   1,
		Storage:         n.storage,
		Applied:         applied,
		MaxSizePerMsg:   256,
		MaxInflightMsgs: 8,
		PreVote:         true,
		CheckQuorum:     true,
		Logger:          testLogger(s.t),
		Rand:            rand.New(rand.NewPCG(s.seed, uint64(n.id)*7919+uint64(s.now))),
	}
	rn, err := NewRawNode(cfg)
	if err != nil {
		s.failf("new raw node %d: %v", n.id, err)
	}
	return rn
}

func (s *sim) tracef(format string, args ...any) {
	line := fmt.Sprintf("t=%d ", s.now) + fmt.Sprintf(format, args...)
	if len(s.trace) >= 400 {
		s.trace = s.trace[1:]
	}
	s.trace = append(s.trace, line)
}

func (s *sim) failf(format string, args ...any) {
	if s.failed {
		return
	}
	s.failed = true
	var sb strings.Builder
	fmt.Fprintf(&sb, "scenario %q seed %d tick %d: ", s.cfg.name, s.seed, s.now)
	fmt.Fprintf(&sb, format, args...)
	sb.WriteString("\n\nnode status:\n")
	for _, id := range s.ids {
		n := s.nodes[id]
		if !n.alive {
			fmt.Fprintf(&sb, "  %d: down\n", id)
			continue
		}
		st := n.rn.Status()
		fmt.Fprintf(&sb, "  %d: %s term=%d lead=%d commit=%d applied=%d last=%d conf=%s\n",
			id, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.LastIndex, st.ConfState)
	}
	sb.WriteString("\nrecent events:\n")
	start := max(0, len(s.trace)-120)
	for _, line := range s.trace[start:] {
		sb.WriteString("  " + line + "\n")
	}
	s.t.Error(sb.String())
}

// ---------------------------------------------------------------------------
// Network

func (s *sim) linkBlocked(from, to NodeID) bool {
	if s.partition[from] != s.partition[to] {
		return true
	}
	if until, ok := s.linkDown[[2]NodeID{from, to}]; ok && until > s.now {
		return true
	}
	return false
}

func (s *sim) send(msgs []Message) {
	for _, m := range msgs {
		s.stats.messages++
		if m.Type == MsgSnap {
			s.stats.snapshotsSent++
		}
		if s.chaos && s.rng.Float64() < s.cfg.dropRate {
			s.stats.dropped++
			s.tracef("drop %s", m)
			s.reportSnapshot(m, false)
			continue
		}
		s.enqueue(m)
		if s.chaos && s.rng.Float64() < s.cfg.dupRate {
			s.enqueue(m)
		}
	}
}

func (s *sim) enqueue(m Message) {
	delay := 1
	if s.chaos && s.cfg.maxDelay > 1 {
		delay += s.rng.IntN(s.cfg.maxDelay)
	}
	s.queue = append(s.queue, simMsg{deliverAt: s.now + delay, msg: m})
}

func (s *sim) deliverDue() {
	var due []Message
	rest := s.queue[:0]
	for _, qm := range s.queue {
		if qm.deliverAt <= s.now {
			due = append(due, qm.msg)
		} else {
			rest = append(rest, qm)
		}
	}
	s.queue = rest
	if s.chaos {
		s.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	}
	for _, m := range due {
		dst := s.nodes[m.To]
		if dst == nil || !dst.alive || s.linkBlocked(m.From, m.To) {
			s.stats.dropped++
			s.reportSnapshot(m, false)
			continue
		}
		if err := dst.rn.Step(m); err != nil {
			s.tracef("step %s: %v", m, err)
		}
		s.reportSnapshot(m, true)
	}
}

// reportSnapshot plays the transport's role of telling the sender whether a
// snapshot transfer completed.
func (s *sim) reportSnapshot(m Message, ok bool) {
	if m.Type != MsgSnap {
		return
	}
	if src := s.nodes[m.From]; src != nil && src.alive {
		src.rn.ReportSnapshot(m.To, ok)
	}
}

// ---------------------------------------------------------------------------
// Node processing (the "application")

func (s *sim) processReady(n *simNode) {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		if !rd.Snapshot.IsEmpty() {
			if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
				s.failf("node %d: apply snapshot %d: %v", n.id, rd.Snapshot.Meta.Index, err)
				return
			}
			n.fsm = decodeAppliedSnapshot(rd.Snapshot.Data)
			n.conf = rd.Snapshot.Meta.ConfState.Clone()
			n.appliedIndex = rd.Snapshot.Meta.Index
			for _, e := range n.fsm {
				s.recordApplied(n, e)
			}
			s.tracef("node %d restored snapshot %d/%d", n.id, rd.Snapshot.Meta.Term, rd.Snapshot.Meta.Index)
		}
		if err := n.storage.Append(rd.Entries); err != nil {
			s.failf("node %d: append %s: %v", n.id, entriesString(rd.Entries), err)
			return
		}
		if !rd.HardState.IsEmpty() {
			n.storage.SetHardState(rd.HardState)
			if rd.MustSync {
				n.syncedHS = rd.HardState
			}
		} else if rd.MustSync {
			n.syncedHS = n.storage.HardState()
		}
		s.send(rd.Messages)

		for _, e := range rd.CommittedEntries {
			if e.Index != n.appliedIndex+1 {
				s.failf("node %d applied index %d after %d", n.id, e.Index, n.appliedIndex)
				return
			}
			if e.Type == EntryConfChange {
				cc, err := UnmarshalConfChange(e.Data)
				if err != nil {
					s.failf("node %d: bad conf change at %d: %v", n.id, e.Index, err)
					return
				}
				n.conf = n.rn.ApplyConfChange(cc)
				s.tracef("node %d applied %s at %d -> %s", n.id, cc, e.Index, n.conf)
			}
			n.fsm = append(n.fsm, e)
			n.appliedIndex = e.Index
			s.recordApplied(n, e)
		}
		for _, rs := range rd.ReadStates {
			s.checkReadState(n, rs)
		}
		n.rn.Advance(rd)
		if s.failed {
			return
		}
		s.maybeSnapshot(n)
	}
}

func (s *sim) recordApplied(n *simNode, e Entry) {
	s.stats.applied++
	if prev, ok := s.committed[e.Index]; ok {
		if prev.Term != e.Term || prev.Type != e.Type || !bytes.Equal(prev.Data, e.Data) {
			s.failf("STATE MACHINE SAFETY: node %d applied %v at index %d but another node applied %v",
				n.id, e, e.Index, prev)
		}
		return
	}
	s.committed[e.Index] = Entry{Index: e.Index, Term: e.Term, Type: e.Type, Data: append([]byte(nil), e.Data...)}
	if e.Type == EntryNormal && len(e.Data) > 0 {
		s.appliedData[string(e.Data)] = true
	}
	if e.Index > s.maxAppliedAny {
		s.maxAppliedAny = e.Index
	}
}

func (s *sim) checkReadState(n *simNode, rs ReadState) {
	key := string(rs.Context)
	want, ok := n.pendingReads[key]
	if !ok {
		// Duplicated MsgReadIndexResp messages legitimately produce repeated
		// read states; applications must ignore contexts they already served.
		if !n.servedReads[key] {
			s.failf("node %d received unknown read state %q", n.id, key)
		}
		return
	}
	delete(n.pendingReads, key)
	n.servedReads[key] = true
	s.stats.readsServed++
	if rs.Index < want {
		s.failf("LINEARIZABLE READ: node %d read index %d is below applied index %d at request time", n.id, rs.Index, want)
	}
}

func (s *sim) maybeSnapshot(n *simNode) {
	if s.cfg.snapshotEvery == 0 || n.appliedIndex == 0 {
		return
	}
	first, _ := n.storage.FirstIndex()
	if n.appliedIndex < first || n.appliedIndex-first+1 < s.cfg.snapshotEvery {
		return
	}
	if _, err := n.storage.CreateSnapshot(n.appliedIndex, n.conf, encodeAppliedSnapshot(n.fsm)); err != nil {
		s.failf("node %d: create snapshot at %d: %v", n.id, n.appliedIndex, err)
		return
	}
	const keep = 3
	if n.appliedIndex > keep && n.appliedIndex-keep > first {
		if err := n.storage.Compact(n.appliedIndex - keep); err != nil {
			s.failf("node %d: compact to %d: %v", n.id, n.appliedIndex-keep, err)
			return
		}
	}
	s.stats.snapshotsTaken++
	s.tracef("node %d took snapshot at %d", n.id, n.appliedIndex)
}

func (s *sim) crash(n *simNode) {
	s.stats.crashes++
	s.tracef("node %d crashes (term %d commit %d)", n.id, n.lastTerm, n.lastCommit)
	n.alive = false
	n.rn = nil
	n.downAt = s.now
	// Lose the hard-state updates that were not fsynced (commit-only
	// updates); entries are always synced before messages go out.
	n.storage.SetHardState(n.syncedHS)
	// Reads that were in flight are abandoned; late responses for them are
	// legal and must be ignored rather than reported as unknown.
	for ctx := range n.pendingReads {
		n.servedReads[ctx] = true
	}
	n.pendingReads = make(map[string]Index)
}

func (s *sim) restart(n *simNode) {
	snap, _ := n.storage.Snapshot()
	n.fsm = decodeAppliedSnapshot(snap.Data)
	n.conf = snap.Meta.ConfState.Clone()
	n.appliedIndex = snap.Meta.Index
	n.rn = s.newRawNode(n, snap.Meta.Index)
	if s.failed {
		return
	}
	n.alive = true
	n.upSince = s.now
	st := n.rn.Status()
	if st.Term < n.syncedHS.Term {
		s.failf("node %d restarted with term %d below synced term %d", n.id, st.Term, n.syncedHS.Term)
	}
	n.lastTerm = st.Term
	n.lastCommit = st.Commit
	s.tracef("node %d restarts (term %d commit %d applied %d)", n.id, st.Term, st.Commit, st.Applied)
}

// ---------------------------------------------------------------------------
// Fault injection and client activity

func (s *sim) every(mean int) bool {
	return mean > 0 && s.rng.IntN(mean) == 0
}

func (s *sim) injectFaults() {
	if s.partitionUntil != 0 && s.now >= s.partitionUntil {
		s.partition = make(map[NodeID]int)
		s.partitionUntil = 0
		s.tracef("partition healed")
	}
	if s.partitionUntil == 0 && s.every(s.cfg.partitionEvery) {
		s.stats.partitions++
		groups := 2 + s.rng.IntN(2)
		s.partition = make(map[NodeID]int)
		desc := make([]string, 0, len(s.ids))
		for _, id := range s.ids {
			s.partition[id] = s.rng.IntN(groups)
			desc = append(desc, fmt.Sprintf("%d:%d", id, s.partition[id]))
		}
		s.partitionUntil = s.now + s.cfg.electionTick() + s.rng.IntN(4*s.cfg.electionTick())
		s.tracef("partition %v until %d", desc, s.partitionUntil)
	}
	if s.every(s.cfg.linkFlapEvery) {
		from := s.ids[s.rng.IntN(len(s.ids))]
		to := s.ids[s.rng.IntN(len(s.ids))]
		if from != to {
			s.linkDown[[2]NodeID{from, to}] = s.now + 1 + s.rng.IntN(3*s.cfg.electionTick())
			s.tracef("link %d->%d down", from, to)
		}
	}
	if s.every(s.cfg.crashEvery) {
		id := s.ids[s.rng.IntN(len(s.ids))]
		if n := s.nodes[id]; n.alive {
			s.crash(n)
		}
	}
	for _, id := range s.ids {
		n := s.nodes[id]
		if !n.alive && s.now-n.downAt >= 1+s.rng.IntN(s.cfg.maxDowntime) {
			s.restart(n)
		}
	}
}

func (s *sim) currentLeader() *simNode {
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.alive && n.rn.Status().State == StateLeader {
			return n
		}
	}
	return nil
}

func (s *sim) randomAlive() *simNode {
	var alive []*simNode
	for _, id := range s.ids {
		if n := s.nodes[id]; n.alive {
			alive = append(alive, n)
		}
	}
	if len(alive) == 0 {
		return nil
	}
	return alive[s.rng.IntN(len(alive))]
}

func (s *sim) clientActivity() {
	if s.rng.Float64() < s.cfg.proposeRate {
		if n := s.randomAlive(); n != nil {
			s.stats.proposals++
			data := fmt.Appendf(nil, "p%d-%d", s.now, s.stats.proposals)
			if _, err := n.rn.Propose(data); err == nil {
				s.stats.proposalsAccepted++
			}
		}
	}
	if s.rng.Float64() < s.cfg.readRate {
		if n := s.randomAlive(); n != nil {
			s.nextReadCtx++
			ctx := fmt.Sprintf("r%d", s.nextReadCtx)
			n.pendingReads[ctx] = s.maxAppliedAny
			s.stats.readsRequested++
			n.rn.ReadIndex([]byte(ctx))
		}
	}
	if s.every(s.cfg.transferEvery) {
		if lead := s.currentLeader(); lead != nil {
			cs := lead.rn.Status().ConfState
			if len(cs.Voters) > 1 {
				target := cs.Voters[s.rng.IntN(len(cs.Voters))]
				if target != lead.id {
					s.stats.transfers++
					s.tracef("transfer leadership %d -> %d", lead.id, target)
					_ = lead.rn.TransferLeader(target)
				}
			}
		}
	}
	if s.every(s.cfg.membershipEvery) {
		s.proposeMembershipChange()
	}
}

func (s *sim) proposeMembershipChange() {
	lead := s.currentLeader()
	if lead == nil {
		return
	}
	cs := lead.rn.Status().ConfState
	var candidates []ConfChange
	for _, id := range s.ids {
		if !cs.Contains(id) {
			candidates = append(candidates, ConfChange{Type: ConfChangeAddLearner, NodeID: id, Context: fmt.Appendf(nil, "addr-%d", id)})
		}
	}
	for _, id := range cs.Learners {
		candidates = append(candidates, ConfChange{Type: ConfChangeAddVoter, NodeID: id})
		candidates = append(candidates, ConfChange{Type: ConfChangeRemoveNode, NodeID: id})
	}
	if len(cs.Voters) > 3 {
		for _, id := range cs.Voters {
			candidates = append(candidates, ConfChange{Type: ConfChangeRemoveNode, NodeID: id})
		}
	}
	if len(candidates) == 0 {
		return
	}
	cc := candidates[s.rng.IntN(len(candidates))]
	if _, err := lead.rn.ProposeConfChange(cc); err != nil {
		s.tracef("conf change %s rejected: %v", cc, err)
		return
	}
	s.stats.confChanges++
	s.tracef("leader %d proposed %s", lead.id, cc)
}

// ---------------------------------------------------------------------------
// Invariants

func (s *sim) checkInvariants() {
	for _, id := range s.ids {
		n := s.nodes[id]
		if !n.alive {
			continue
		}
		st := n.rn.Status()
		if st.Term < n.lastTerm {
			s.failf("MONOTONIC TERM: node %d went from term %d to %d", id, n.lastTerm, st.Term)
		}
		if st.Commit < n.lastCommit {
			s.failf("MONOTONIC COMMIT: node %d went from commit %d to %d", id, n.lastCommit, st.Commit)
		}
		n.lastTerm, n.lastCommit = st.Term, st.Commit

		if st.State == StateLeader {
			if prev, ok := s.leaders[st.Term]; ok {
				if prev != id {
					s.failf("ELECTION SAFETY: nodes %d and %d are both leaders in term %d", prev, id, st.Term)
				}
			} else {
				s.leaders[st.Term] = id
				s.stats.elections++
				s.tracef("node %d is leader of term %d", id, st.Term)
				s.checkLeaderCompleteness(n, st)
			}
		}
	}
	s.checkLogMatching()
}

func (s *sim) checkLeaderCompleteness(n *simNode, st Status) {
	for idx, e := range s.committed {
		if e.Term >= st.Term {
			continue
		}
		if idx > st.LastIndex {
			s.failf("LEADER COMPLETENESS: leader %d (term %d) lacks applied entry %v", n.id, st.Term, e)
			return
		}
		term, err := n.rn.raft.raftLog.term(idx)
		if err == nil {
			if term != e.Term {
				s.failf("LEADER COMPLETENESS: leader %d has term %d at %d, applied entry has term %d", n.id, term, idx, e.Term)
				return
			}
			continue
		}
		// Compacted on the leader: it must have applied it itself.
		found := false
		for _, a := range n.fsm {
			if a.Index == idx {
				found = a.Term == e.Term
				break
			}
		}
		if !found {
			s.failf("LEADER COMPLETENESS: leader %d compacted index %d without having applied %v", n.id, idx, e)
			return
		}
	}
}

func (s *sim) checkLogMatching() {
	alive := make([]*simNode, 0, len(s.ids))
	for _, id := range s.ids {
		if n := s.nodes[id]; n.alive {
			alive = append(alive, n)
		}
	}
	for i := range alive {
		for j := i + 1; j < len(alive); j++ {
			a, b := alive[i], alive[j]
			la, lb := a.rn.raft.raftLog, b.rn.raft.raftLog
			hi := min(la.lastIndex(), lb.lastIndex())
			// The committed prefix is covered by state machine safety; only
			// the uncommitted suffixes need the pairwise check.
			lo := max(la.firstIndex(), lb.firstIndex(), min(la.committed, lb.committed)+1)
			if lo > hi {
				continue
			}
			var match Index
			for idx := hi; idx >= lo; idx-- {
				ta, erra := la.term(idx)
				tb, errb := lb.term(idx)
				if erra == nil && errb == nil && ta == tb {
					match = idx
					break
				}
			}
			if match == 0 {
				continue
			}
			ea, erra := la.slice(lo, match+1, ^uint64(0))
			eb, errb := lb.slice(lo, match+1, ^uint64(0))
			if erra != nil || errb != nil {
				s.failf("log matching: slice [%d,%d]: %v / %v", lo, match, erra, errb)
				return
			}
			for k := range ea {
				if ea[k].Term != eb[k].Term || !bytes.Equal(ea[k].Data, eb[k].Data) {
					s.failf("LOG MATCHING: nodes %d and %d agree on %d/%d but differ at %d: %v vs %v",
						a.id, b.id, la.zeroTermOnOutOfBounds(la.term(match)), match, ea[k].Index, ea[k], eb[k])
					return
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Driver

func (s *sim) step() {
	defer func() {
		if r := recover(); r != nil {
			s.failf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	s.now++
	if s.chaos {
		s.injectFaults()
	} else {
		for _, id := range s.ids {
			if n := s.nodes[id]; !n.alive {
				s.restart(n)
			}
		}
	}
	for _, id := range s.ids {
		if n := s.nodes[id]; n.alive {
			n.rn.Tick()
		}
	}
	if s.chaos {
		s.clientActivity()
	}
	s.deliverDue()
	for _, id := range s.ids {
		if n := s.nodes[id]; n.alive {
			s.processReady(n)
		}
	}
	// Readies can generate immediate work on other nodes only through the
	// queue, which is delivered next tick; one pass is therefore complete.
	s.checkInvariants()
}

func (s *sim) run() {
	s.chaos = true
	ticks := s.cfg.ticks
	if *simTicks > 0 {
		ticks = *simTicks
	}
	for i := 0; i < ticks && !s.failed; i++ {
		s.step()
	}
	if s.failed {
		return
	}

	// Heal everything and check liveness.
	s.chaos = false
	s.partition = make(map[NodeID]int)
	s.partitionUntil = 0
	s.linkDown = make(map[[2]NodeID]int)
	s.tracef("chaos phase over; healing")

	converged := false
	var lead *simNode
	for i := 0; i < s.cfg.convergeTicks && !s.failed; i++ {
		s.step()
		if lead = s.currentLeader(); lead != nil && s.converged(lead) {
			converged = true
			break
		}
	}
	if s.failed {
		return
	}
	if !converged {
		s.failf("LIVENESS: cluster did not converge within %d ticks after healing", s.cfg.convergeTicks)
		return
	}

	// New proposals must commit on every member. A proposal can be refused
	// while a leadership transfer started during the chaos phase is still
	// pending, and one accepted by a leader that loses leadership right
	// afterwards is dropped, so resubmit like a client would until every
	// final command has been applied.
	const finalProposals = 5
	pending := make(map[int]Term) // proposal -> term of the leader it was given to
	for i := 0; i < s.cfg.convergeTicks && !s.failed; i++ {
		lead = s.currentLeader()
		if lead != nil {
			term := lead.rn.BasicStatus().Term
			for k := range finalProposals {
				data := fmt.Appendf(nil, "final-%d", k)
				if s.appliedData[string(data)] {
					continue
				}
				if t, ok := pending[k]; ok && t == term {
					continue
				}
				if _, err := lead.rn.Propose(data); err == nil {
					pending[k] = term
				}
			}
		}
		s.step()
		done := true
		for k := range finalProposals {
			if !s.appliedData[fmt.Sprintf("final-%d", k)] {
				done = false
				break
			}
		}
		if done && lead != nil && s.converged(lead) {
			s.checkStateMachinesEqual()
			return
		}
	}
	if !s.failed {
		s.failf("LIVENESS: final proposals did not commit on every member within %d ticks", s.cfg.convergeTicks)
	}
}

func (s *sim) members(lead *simNode) []*simNode {
	cs := lead.rn.Status().ConfState
	var out []*simNode
	for _, id := range s.ids {
		if cs.Contains(id) {
			out = append(out, s.nodes[id])
		}
	}
	return out
}

func (s *sim) converged(lead *simNode) bool {
	st := lead.rn.Status()
	if !st.ConfState.IsVoter(lead.id) {
		return false
	}
	for _, n := range s.members(lead) {
		if !n.alive {
			return false
		}
		ns := n.rn.Status()
		if ns.Leader != lead.id || ns.Term != st.Term || ns.Applied != st.Commit {
			return false
		}
	}
	return true
}

func (s *sim) allMembersApplied(idx Index) bool {
	lead := s.currentLeader()
	if lead == nil {
		return false
	}
	for _, n := range s.members(lead) {
		if !n.alive || n.appliedIndex < idx {
			return false
		}
	}
	return true
}

func (s *sim) checkStateMachinesEqual() {
	lead := s.currentLeader()
	if lead == nil {
		s.failf("no leader at the end")
		return
	}
	for _, n := range s.members(lead) {
		if n.appliedIndex != lead.appliedIndex {
			s.failf("node %d applied %d, leader %d applied %d", n.id, n.appliedIndex, lead.id, lead.appliedIndex)
			return
		}
		// Compare the overlap of the two state machines.
		li := len(lead.fsm) - 1
		ni := len(n.fsm) - 1
		for li >= 0 && ni >= 0 {
			a, b := lead.fsm[li], n.fsm[ni]
			if a.Index != b.Index || a.Term != b.Term || !bytes.Equal(a.Data, b.Data) {
				s.failf("state machines differ: leader %d has %v, node %d has %v", lead.id, a, n.id, b)
				return
			}
			li--
			ni--
		}
	}
}

// ---------------------------------------------------------------------------
// Scenarios

var simScenarios = []simConfig{
	{
		name: "three-nodes", voters: 3, ticks: 1500,
		dropRate: 0.05, dupRate: 0.02, maxDelay: 3,
		partitionEvery: 150, linkFlapEvery: 60, crashEvery: 200, maxDowntime: 40,
		proposeRate: 0.5, readRate: 0.1, transferEvery: 300,
		convergeTicks: 300,
	},
	{
		name: "five-nodes-chaos", voters: 5, ticks: 2000,
		dropRate: 0.1, dupRate: 0.05, maxDelay: 4,
		partitionEvery: 100, linkFlapEvery: 30, crashEvery: 100, maxDowntime: 60,
		proposeRate: 0.7, readRate: 0.2, transferEvery: 150,
		snapshotEvery: 40,
		convergeTicks: 400,
	},
	{
		name: "membership", voters: 3, spares: 3, ticks: 2500,
		dropRate: 0.05, dupRate: 0.02, maxDelay: 3,
		partitionEvery: 200, linkFlapEvery: 80, crashEvery: 150, maxDowntime: 50,
		proposeRate: 0.4, readRate: 0.1, transferEvery: 250,
		snapshotEvery: 30, membershipEvery: 60,
		convergeTicks: 500,
	},
}

func TestSimulation(t *testing.T) {
	t.Parallel()
	for _, cfg := range simScenarios {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			seeds := *simSeeds
			if testing.Short() {
				seeds = min(seeds, 8)
			}
			var total simStats
			runOne := func(seed uint64) bool {
				s := newSim(t, cfg, seed)
				s.run()
				total.add(s.stats)
				return !s.failed
			}
			if *simSeed != 0 {
				if !runOne(*simSeed) {
					t.FailNow()
				}
			} else {
				for seed := uint64(1); seed <= uint64(seeds); seed++ {
					if !runOne(seed) {
						t.FailNow()
					}
				}
			}
			t.Logf("%s: %s", cfg.name, total.String())
		})
	}
}

func (st *simStats) add(o simStats) {
	st.proposals += o.proposals
	st.proposalsAccepted += o.proposalsAccepted
	st.applied += o.applied
	st.readsRequested += o.readsRequested
	st.readsServed += o.readsServed
	st.elections += o.elections
	st.snapshotsSent += o.snapshotsSent
	st.snapshotsTaken += o.snapshotsTaken
	st.crashes += o.crashes
	st.partitions += o.partitions
	st.confChanges += o.confChanges
	st.transfers += o.transfers
	st.messages += o.messages
	st.dropped += o.dropped
}

func (st simStats) String() string {
	return fmt.Sprintf("proposals=%d accepted=%d applied=%d reads=%d/%d elections=%d snapshots=%d/%d crashes=%d partitions=%d confchanges=%d transfers=%d messages=%d dropped=%d",
		st.proposals, st.proposalsAccepted, st.applied, st.readsServed, st.readsRequested, st.elections,
		st.snapshotsSent, st.snapshotsTaken, st.crashes, st.partitions, st.confChanges, st.transfers, st.messages, st.dropped)
}

// Keep slices imported for scenario helpers that sort ids.
var _ = slices.Sort[[]NodeID]
