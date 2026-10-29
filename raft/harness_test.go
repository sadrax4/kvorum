package raft

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
)

// testLogger returns a logger that is silent unless RAFT_TEST_LOG is set.
func testLogger(tb testing.TB) *slog.Logger {
	tb.Helper()
	if os.Getenv("RAFT_TEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newTestStorage returns a MemoryStorage whose initial membership is the
// given voters, installed through an empty snapshot at index 0.
func newTestStorage(voters ...NodeID) *MemoryStorage {
	ms := NewMemoryStorage()
	if len(voters) > 0 {
		if err := ms.ApplySnapshot(Snapshot{Meta: SnapshotMeta{ConfState: ConfState{Voters: voters}}}); err != nil {
			panic(err)
		}
	}
	return ms
}

func testConfig(tb testing.TB, id NodeID, storage Storage, seed uint64) *Config {
	tb.Helper()
	return &Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
		CheckQuorum:     true,
		Logger:          testLogger(tb),
		Rand:            rand.New(rand.NewPCG(seed, uint64(id))),
	}
}

func mustNewRawNode(tb testing.TB, cfg *Config) *RawNode {
	tb.Helper()
	rn, err := NewRawNode(cfg)
	if err != nil {
		tb.Fatalf("NewRawNode(%d): %v", cfg.ID, err)
	}
	return rn
}

// appNode is a minimal application around a RawNode: it persists Ready
// output into a MemoryStorage and applies committed entries.
type appNode struct {
	id        NodeID
	rn        *RawNode
	storage   *MemoryStorage
	applied   []Entry
	conf      ConfState
	readyDone int
	// readStates collects confirmed reads.
	readStates []ReadState
}

func newAppNode(tb testing.TB, id NodeID, storage *MemoryStorage, seed uint64, mutate func(*Config)) *appNode {
	tb.Helper()
	cfg := testConfig(tb, id, storage, seed)
	if mutate != nil {
		mutate(cfg)
	}
	return &appNode{id: id, rn: mustNewRawNode(tb, cfg), storage: storage}
}

// processReady handles one Ready and returns the messages to send. It mirrors
// the contract documented on Ready: snapshot first, then entries and hard
// state, then messages, then apply.
func (n *appNode) processReady(tb testing.TB) []Message {
	tb.Helper()
	if !n.rn.HasReady() {
		return nil
	}
	rd := n.rn.Ready()
	if !rd.Snapshot.IsEmpty() {
		if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
			tb.Fatalf("node %d: apply snapshot: %v", n.id, err)
		}
		n.applied = decodeAppliedSnapshot(rd.Snapshot.Data)
		n.conf = rd.Snapshot.Meta.ConfState.Clone()
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		tb.Fatalf("node %d: append: %v", n.id, err)
	}
	if !rd.HardState.IsEmpty() {
		n.storage.SetHardState(rd.HardState)
	}
	for _, e := range rd.CommittedEntries {
		if e.Type == EntryConfChange {
			cc, err := UnmarshalConfChange(e.Data)
			if err != nil {
				tb.Fatalf("node %d: bad conf change at %d: %v", n.id, e.Index, err)
			}
			n.conf = n.rn.ApplyConfChange(cc)
		}
		n.applied = append(n.applied, e)
	}
	n.readStates = append(n.readStates, rd.ReadStates...)
	n.rn.Advance(rd)
	n.readyDone++
	return rd.Messages
}

// drain processes Readies until the node is quiescent, collecting messages.
func (n *appNode) drain(tb testing.TB) []Message {
	tb.Helper()
	var out []Message
	for n.rn.HasReady() {
		out = append(out, n.processReady(tb)...)
	}
	return out
}

// appliedNormal returns the applied entries that carry commands (skipping
// the leader no-ops and configuration changes).
func (n *appNode) appliedNormal() []Entry {
	var out []Entry
	for _, e := range n.applied {
		if e.Type == EntryNormal && len(e.Data) > 0 {
			out = append(out, e)
		}
	}
	return out
}

// snapshotApplied compacts the node's log at its applied index, encoding the
// applied entries as the snapshot payload.
func (n *appNode) snapshotApplied(tb testing.TB, keep Index) {
	tb.Helper()
	st := n.rn.Status()
	if st.Applied == 0 {
		return
	}
	if _, err := n.storage.CreateSnapshot(st.Applied, st.ConfState, encodeAppliedSnapshot(n.applied)); err != nil {
		tb.Fatalf("node %d: create snapshot at %d: %v", n.id, st.Applied, err)
	}
	if st.Applied > keep {
		if err := n.storage.Compact(st.Applied - keep); err != nil {
			tb.Fatalf("node %d: compact to %d: %v", n.id, st.Applied-keep, err)
		}
	}
}

// encodeAppliedSnapshot and decodeAppliedSnapshot serialise the test state
// machine (the list of applied entries) so that snapshots can be restored.
func encodeAppliedSnapshot(applied []Entry) []byte {
	var buf []byte
	buf = appendUint64(buf, uint64(len(applied)))
	for _, e := range applied {
		buf = appendUint64(buf, uint64(e.Index))
		buf = appendUint64(buf, uint64(e.Term))
		buf = append(buf, byte(e.Type))
		buf = appendUint64(buf, uint64(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

func decodeAppliedSnapshot(data []byte) []Entry {
	if len(data) == 0 {
		return nil
	}
	n, data := readUint64(data)
	out := make([]Entry, 0, n)
	for range n {
		var e Entry
		var v uint64
		v, data = readUint64(data)
		e.Index = Index(v)
		v, data = readUint64(data)
		e.Term = Term(v)
		e.Type = EntryType(data[0])
		data = data[1:]
		v, data = readUint64(data)
		if v > 0 {
			e.Data = append([]byte(nil), data[:v]...)
			data = data[v:]
		}
		out = append(out, e)
	}
	return out
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func readUint64(b []byte) (uint64, []byte) {
	v := uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
	return v, b[8:]
}

// network is a deterministic in-memory cluster for unit tests. Messages are
// delivered immediately and in order unless a link is cut or configured to
// drop. A nil node acts as a black hole.
type network struct {
	t       testing.TB
	ids     []NodeID
	nodes   map[NodeID]*appNode
	cut     map[[2]NodeID]bool
	dropAll map[NodeID]bool
	ignore  map[MessageType]bool
	rng     *rand.Rand
	// pending holds messages that have been handed to send but not yet
	// delivered; it exists so that tests can inspect traffic.
	delivered []Message
}

type networkOption func(*Config)

func withoutPreVote() networkOption     { return func(c *Config) { c.PreVote = false } }
func withoutCheckQuorum() networkOption { return func(c *Config) { c.CheckQuorum = false } }

// newNetwork bootstraps a cluster of the given ids, with the bootstrap
// entries persisted and applied on every node.
func newNetwork(tb testing.TB, ids []NodeID, opts ...networkOption) *network {
	tb.Helper()
	nw := &network{
		t:       tb,
		ids:     slices.Clone(ids),
		nodes:   make(map[NodeID]*appNode),
		cut:     make(map[[2]NodeID]bool),
		dropAll: make(map[NodeID]bool),
		ignore:  make(map[MessageType]bool),
		rng:     rand.New(rand.NewPCG(1, 2)),
	}
	slices.Sort(nw.ids)
	peers := make([]Peer, 0, len(ids))
	for _, id := range nw.ids {
		peers = append(peers, Peer{ID: id})
	}
	for _, id := range nw.ids {
		n := newAppNode(tb, id, NewMemoryStorage(), 7, func(c *Config) {
			for _, o := range opts {
				o(c)
			}
		})
		if err := n.rn.Bootstrap(peers); err != nil {
			tb.Fatalf("bootstrap %d: %v", id, err)
		}
		n.drain(tb)
		nw.nodes[id] = n
	}
	return nw
}

func (nw *network) node(id NodeID) *appNode { return nw.nodes[id] }

// flush processes Readies and delivers messages until nothing is left.
func (nw *network) flush() {
	nw.t.Helper()
	for {
		var msgs []Message
		for _, id := range nw.ids {
			if n := nw.nodes[id]; n != nil {
				msgs = append(msgs, n.drain(nw.t)...)
			}
		}
		if len(msgs) == 0 {
			return
		}
		nw.deliver(msgs)
	}
}

func (nw *network) deliver(msgs []Message) {
	nw.t.Helper()
	for _, m := range msgs {
		if nw.ignore[m.Type] || nw.cut[[2]NodeID{m.From, m.To}] || nw.dropAll[m.To] || nw.dropAll[m.From] {
			continue
		}
		dst := nw.nodes[m.To]
		if dst == nil {
			continue
		}
		nw.delivered = append(nw.delivered, m)
		if err := dst.rn.Step(m); err != nil {
			nw.t.Logf("step %s: %v", m, err)
		}
	}
}

// tick advances one node by one tick and flushes.
func (nw *network) tick(id NodeID) {
	nw.t.Helper()
	nw.nodes[id].rn.Tick()
	nw.flush()
}

// tickAll advances every live node by n ticks, flushing after each tick.
func (nw *network) tickAll(n int) {
	nw.t.Helper()
	for range n {
		for _, id := range nw.ids {
			if node := nw.nodes[id]; node != nil {
				node.rn.Tick()
			}
		}
		nw.flush()
	}
}

func (nw *network) campaign(id NodeID) {
	nw.t.Helper()
	if err := nw.nodes[id].rn.Campaign(); err != nil {
		nw.t.Fatalf("campaign %d: %v", id, err)
	}
	nw.flush()
}

func (nw *network) propose(id NodeID, data string) (Index, error) {
	nw.t.Helper()
	idx, err := nw.nodes[id].rn.Propose([]byte(data))
	nw.flush()
	return idx, err
}

func (nw *network) mustPropose(id NodeID, data string) Index {
	nw.t.Helper()
	idx, err := nw.propose(id, data)
	if err != nil {
		nw.t.Fatalf("propose on %d: %v", id, err)
	}
	return idx
}

// isolate drops every message to and from the node.
func (nw *network) isolate(id NodeID) { nw.dropAll[id] = true }

// cutLink drops messages in both directions between a and b.
func (nw *network) cutLink(a, b NodeID) {
	nw.cut[[2]NodeID{a, b}] = true
	nw.cut[[2]NodeID{b, a}] = true
}

// heal removes every partition.
func (nw *network) heal() {
	nw.cut = make(map[[2]NodeID]bool)
	nw.dropAll = make(map[NodeID]bool)
}

// leader returns the single leader among live nodes, or None.
func (nw *network) leader() NodeID {
	lead := None
	for _, id := range nw.ids {
		n := nw.nodes[id]
		if n == nil {
			continue
		}
		if st := n.rn.Status(); st.State == StateLeader {
			if lead != None {
				nw.t.Fatalf("two leaders: %d and %d", lead, id)
			}
			lead = id
		}
	}
	return lead
}

// electLeader runs ticks until some node becomes leader.
func (nw *network) electLeader() NodeID {
	nw.t.Helper()
	for range 200 {
		if l := nw.leader(); l != None {
			return l
		}
		nw.tickAll(1)
	}
	nw.t.Fatalf("no leader elected")
	return None
}

func (nw *network) status(id NodeID) Status { return nw.nodes[id].rn.Status() }

// crash stops a node, keeping its storage for a later restart.
func (nw *network) crash(id NodeID) {
	nw.nodes[id] = nil
}

// restart recreates a node from its storage, with Applied set to the
// snapshot index (the application state is restored from the snapshot).
func (nw *network) restart(id NodeID, storage *MemoryStorage, opts ...networkOption) {
	nw.t.Helper()
	snap, _ := storage.Snapshot()
	n := newAppNode(nw.t, id, storage, 7, func(c *Config) {
		c.Applied = snap.Meta.Index
		for _, o := range opts {
			o(c)
		}
	})
	n.applied = decodeAppliedSnapshot(snap.Data)
	n.conf = snap.Meta.ConfState.Clone()
	nw.nodes[id] = n
}

func entriesString(ents []Entry) string {
	s := "["
	for i, e := range ents {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%d/%d", e.Term, e.Index)
	}
	return s + "]"
}
