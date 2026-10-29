package transport

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

// allMessageTypes lists every valid message type in order.
var allMessageTypes = []raft.MessageType{
	raft.MsgPreVote, raft.MsgPreVoteResp, raft.MsgVote, raft.MsgVoteResp,
	raft.MsgApp, raft.MsgAppResp, raft.MsgHeartbeat, raft.MsgHeartbeatResp,
	raft.MsgSnap, raft.MsgTimeoutNow, raft.MsgReadIndex, raft.MsgReadIndexResp,
}

// sampleMessage builds a message of type mt with every field set to a value
// that is meaningful for that type, plus a few that are not, to confirm the
// codec is field-complete rather than type-aware.
func sampleMessage(mt raft.MessageType) raft.Message {
	m := raft.Message{
		Type:          mt,
		From:          7,
		To:            9,
		Term:          42,
		LastLogIndex:  100,
		LastLogTerm:   41,
		PrevLogIndex:  99,
		PrevLogTerm:   40,
		Commit:        98,
		Reject:        true,
		MatchIndex:    97,
		RejectHint:    96,
		ConflictTerm:  39,
		ConflictIndex: 95,
		Context:       []byte("ctx"),
		ReadIndex:     94,
		Transfer:      true,
	}
	switch mt {
	case raft.MsgApp:
		m.Entries = []raft.Entry{
			{Index: 100, Term: 42, Type: raft.EntryNormal, Data: []byte("put a=1")},
			{Index: 101, Term: 42, Type: raft.EntryConfChange, Data: []byte{1, 2, 3}},
			{Index: 102, Term: 42, Type: raft.EntryNormal},
		}
	case raft.MsgSnap:
		m.Snapshot = &raft.Snapshot{Meta: raft.SnapshotMeta{
			Index:     100,
			Term:      42,
			ConfState: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{4}},
		}}
	}
	return m
}

// expectedAfterRoundTrip returns what FromProto(ToProto(m)) must yield: the
// same message with the snapshot payload stripped.
func expectedAfterRoundTrip(m raft.Message) raft.Message {
	if m.Snapshot != nil {
		snap := *m.Snapshot
		snap.Data = nil
		m.Snapshot = &snap
	}
	return m
}

// roundTrip encodes m, marshals and unmarshals the wire bytes, and decodes.
func roundTrip(t *testing.T, m raft.Message) raft.Message {
	t.Helper()
	wire, err := proto.Marshal(ToProto(m))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var pm raftpb.Message
	if err := proto.Unmarshal(wire, &pm); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, err := FromProto(&pm)
	if err != nil {
		t.Fatalf("FromProto: %v", err)
	}
	return got
}

func TestCodecRoundTripEveryType(t *testing.T) {
	t.Parallel()
	for _, mt := range allMessageTypes {
		t.Run(mt.String(), func(t *testing.T) {
			t.Parallel()
			m := sampleMessage(mt)
			if m.Snapshot != nil {
				m.Snapshot.Data = []byte("payload that must not travel in the header")
			}
			got := roundTrip(t, m)
			if want := expectedAfterRoundTrip(m); !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip mismatch\n got: %+v\nwant: %+v", got, want)
			}
		})
	}
}

func TestToProtoEncodesSnapshotMetaOnlyForMsgSnap(t *testing.T) {
	t.Parallel()
	m := sampleMessage(raft.MsgApp)
	m.Snapshot = &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 5, Term: 1}}
	if pm := ToProto(m); pm.GetSnapshotMeta() != nil {
		t.Fatalf("MsgApp carried snapshot metadata: %v", pm.GetSnapshotMeta())
	}
	snap := sampleMessage(raft.MsgSnap)
	snap.Snapshot.Data = make([]byte, 1024)
	pm := ToProto(snap)
	if pm.GetSnapshotMeta() == nil {
		t.Fatal("MsgSnap lost snapshot metadata")
	}
	if n := proto.Size(pm); n > 128 {
		t.Fatalf("MsgSnap header is %d bytes; payload leaked into the header", n)
	}
}

func TestFromProtoRejectsMalformedMessages(t *testing.T) {
	t.Parallel()
	valid := func(mt raft.MessageType) *raftpb.Message { return ToProto(sampleMessage(mt)) }
	tests := []struct {
		name string
		pm   *raftpb.Message
	}{
		{name: "nil", pm: nil},
		{name: "type zero", pm: &raftpb.Message{Type: 0, From: 1, To: 2}},
		{name: "type past last", pm: &raftpb.Message{Type: uint32(raft.MsgReadIndexResp) + 1, From: 1, To: 2}},
		{name: "type huge", pm: &raftpb.Message{Type: 1 << 20, From: 1, To: 2}},
		{name: "nil entry", pm: func() *raftpb.Message {
			pm := valid(raft.MsgApp)
			pm.Entries[1] = nil
			return pm
		}()},
		{name: "entry type out of range", pm: func() *raftpb.Message {
			pm := valid(raft.MsgApp)
			pm.Entries[0].Type = uint32(raft.EntryConfChange) + 1
			return pm
		}()},
		{name: "snapshot without meta", pm: func() *raftpb.Message {
			pm := valid(raft.MsgSnap)
			pm.SnapshotMeta = nil
			return pm
		}()},
		{name: "meta on non-snapshot", pm: func() *raftpb.Message {
			pm := valid(raft.MsgHeartbeat)
			pm.SnapshotMeta = &raftpb.SnapshotMeta{Index: 1, Term: 1}
			return pm
		}()},
		{name: "zero voter id", pm: func() *raftpb.Message {
			pm := valid(raft.MsgSnap)
			pm.SnapshotMeta.ConfState.Voters = []uint64{1, 0}
			return pm
		}()},
		{name: "zero learner id", pm: func() *raftpb.Message {
			pm := valid(raft.MsgSnap)
			pm.SnapshotMeta.ConfState.Learners = []uint64{0}
			return pm
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := FromProto(tc.pm)
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("FromProto error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestFromProtoAcceptsSnapshotWithoutConfState(t *testing.T) {
	t.Parallel()
	pm := &raftpb.Message{
		Type:         uint32(raft.MsgSnap),
		From:         1,
		To:           2,
		SnapshotMeta: &raftpb.SnapshotMeta{Index: 10, Term: 3},
	}
	m, err := FromProto(pm)
	if err != nil {
		t.Fatalf("FromProto: %v", err)
	}
	if m.Snapshot == nil || m.Snapshot.Meta.Index != 10 || m.Snapshot.Meta.Term != 3 {
		t.Fatalf("unexpected snapshot: %+v", m.Snapshot)
	}
	if !m.Snapshot.Meta.ConfState.IsEmpty() || m.Snapshot.Data != nil {
		t.Fatalf("expected empty conf state and data, got %+v", m.Snapshot)
	}
}

// randomMessage draws a message with every field randomised. Slices are nil
// when empty so that the comparison is not sensitive to nil-versus-empty.
func randomMessage(r *rand.Rand) raft.Message {
	bytesOf := func(n int) []byte {
		if n == 0 {
			return nil
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.UintN(256))
		}
		return b
	}
	ids := func(n int) []raft.NodeID {
		if n == 0 {
			return nil
		}
		out := make([]raft.NodeID, n)
		for i := range out {
			out[i] = raft.NodeID(r.Uint64N(1<<32) + 1)
		}
		return out
	}
	mt := allMessageTypes[r.IntN(len(allMessageTypes))]
	m := raft.Message{
		Type:          mt,
		From:          raft.NodeID(r.Uint64()),
		To:            raft.NodeID(r.Uint64()),
		Term:          raft.Term(r.Uint64()),
		LastLogIndex:  raft.Index(r.Uint64()),
		LastLogTerm:   raft.Term(r.Uint64()),
		PrevLogIndex:  raft.Index(r.Uint64()),
		PrevLogTerm:   raft.Term(r.Uint64()),
		Commit:        raft.Index(r.Uint64()),
		Reject:        r.IntN(2) == 1,
		MatchIndex:    raft.Index(r.Uint64()),
		RejectHint:    raft.Index(r.Uint64()),
		ConflictTerm:  raft.Term(r.Uint64()),
		ConflictIndex: raft.Index(r.Uint64()),
		Context:       bytesOf(r.IntN(16)),
		ReadIndex:     raft.Index(r.Uint64()),
		Transfer:      r.IntN(2) == 1,
	}
	if n := r.IntN(6); n > 0 {
		m.Entries = make([]raft.Entry, n)
		for i := range m.Entries {
			m.Entries[i] = raft.Entry{
				Index: raft.Index(r.Uint64()),
				Term:  raft.Term(r.Uint64()),
				Type:  raft.EntryType(r.IntN(2)),
				Data:  bytesOf(r.IntN(64)),
			}
		}
	}
	if mt == raft.MsgSnap {
		m.Snapshot = &raft.Snapshot{
			Meta: raft.SnapshotMeta{
				Index:     raft.Index(r.Uint64()),
				Term:      raft.Term(r.Uint64()),
				ConfState: raft.ConfState{Voters: ids(r.IntN(5)), Learners: ids(r.IntN(3))},
			},
			Data: bytesOf(r.IntN(128)),
		}
	}
	return m
}

func TestCodecRoundTripProperty(t *testing.T) {
	t.Parallel()
	const seed, iterations = 20261029, 2000
	r := rand.New(rand.NewPCG(seed, seed+1))
	for i := range iterations {
		m := randomMessage(r)
		got := roundTrip(t, m)
		if want := expectedAfterRoundTrip(m); !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: round trip mismatch\n got: %+v\nwant: %+v", i, got, want)
		}
	}
}
