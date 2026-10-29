package raft

import (
	"bytes"
	"errors"
	"testing"
)

func TestConfChangeMarshalRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []ConfChange{
		{Type: ConfChangeAddVoter, NodeID: 1},
		{Type: ConfChangeAddLearner, NodeID: 1 << 40, Context: []byte("10.0.0.1:2380")},
		{Type: ConfChangeRemoveNode, NodeID: 7, Context: []byte{}},
	}
	for _, cc := range cases {
		got, err := UnmarshalConfChange(cc.Marshal())
		if err != nil {
			t.Fatalf("%s: %v", cc, err)
		}
		if got.Type != cc.Type || got.NodeID != cc.NodeID || !bytes.Equal(got.Context, cc.Context) {
			t.Fatalf("round trip %s -> %s", cc, got)
		}
	}
	bad := [][]byte{
		nil,
		{1, 2, 3},
		append(ConfChange{Type: ConfChangeAddVoter, NodeID: 1}.Marshal(), 0xFF),
		ConfChange{Type: ConfChangeType(9), NodeID: 1}.Marshal(),
		ConfChange{Type: ConfChangeAddVoter, NodeID: 0}.Marshal(),
	}
	for i, b := range bad {
		if _, err := UnmarshalConfChange(b); err == nil {
			t.Errorf("case %d: malformed input accepted", i)
		}
	}
}

func TestConfStateMarshalRoundTrip(t *testing.T) {
	t.Parallel()
	cs := ConfState{Voters: []NodeID{3, 1, 2}, Learners: []NodeID{9}}
	b, err := cs.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got ConfState
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(cs) {
		t.Fatalf("round trip %s -> %s", cs, got)
	}
	if err := got.UnmarshalBinary(b[:len(b)-1]); err == nil {
		t.Fatal("truncated input accepted")
	}
	if err := got.UnmarshalBinary(append(b, 0)); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	var empty ConfState
	eb, _ := empty.MarshalBinary()
	if err := got.UnmarshalBinary(eb); err != nil || !got.IsEmpty() {
		t.Fatalf("empty round trip: %s, %v", got, err)
	}
}

func TestConfStateApplyAndValidate(t *testing.T) {
	t.Parallel()
	cs := ConfState{Voters: []NodeID{1, 2, 3}}
	cs = cs.apply(ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	if !cs.IsLearner(4) || cs.IsVoter(4) {
		t.Fatalf("after add learner: %s", cs)
	}
	cs = cs.apply(ConfChange{Type: ConfChangeAddVoter, NodeID: 4})
	if !cs.IsVoter(4) || cs.IsLearner(4) {
		t.Fatalf("after promote: %s", cs)
	}
	cs = cs.apply(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1})
	if cs.Contains(1) || len(cs.Voters) != 3 {
		t.Fatalf("after remove: %s", cs)
	}
	if err := cs.validateChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 2}); !errors.Is(err, ErrInvalidConfChange) {
		t.Errorf("duplicate voter accepted: %v", err)
	}
	if err := cs.validateChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 5}); err != nil {
		t.Errorf("valid learner rejected: %v", err)
	}
	if err := cs.validateChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 5}); !errors.Is(err, ErrInvalidConfChange) {
		t.Errorf("removing a non-member accepted: %v", err)
	}
	single := ConfState{Voters: []NodeID{1}}
	if err := single.validateChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Errorf("removing the last voter accepted: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStorage()
	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero id", Config{ElectionTick: 10, HeartbeatTick: 1, Storage: ms}},
		{"no storage", Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1}},
		{"election not greater", Config{ID: 1, ElectionTick: 1, HeartbeatTick: 1, Storage: ms}},
		{"zero heartbeat", Config{ID: 1, ElectionTick: 10, Storage: ms}},
	}
	for _, tc := range cases {
		cfg := tc.cfg
		if _, err := NewRawNode(&cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", tc.name, err)
		}
	}
	good := Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: ms}
	if _, err := NewRawNode(&good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if good.MaxSizePerMsg == 0 || good.MaxInflightMsgs == 0 || good.Logger == nil || good.Rand == nil {
		t.Fatal("defaults were not applied")
	}
}

func TestStringers(t *testing.T) {
	t.Parallel()
	if NodeID(0).String() != "none" || NodeID(7).String() != "7" {
		t.Fatal("NodeID.String")
	}
	for s := StateFollower; s <= StateLeader; s++ {
		if s.String() == "" {
			t.Fatalf("state %d has no name", s)
		}
	}
	for m := MsgPreVote; m <= MsgReadIndexResp; m++ {
		if m.String() == "" || m.String() == "invalid" {
			t.Fatalf("message type %d has no name", m)
		}
	}
	msg := Message{Type: MsgApp, From: 1, To: 2, Term: 3, PrevLogIndex: 4, PrevLogTerm: 2, Commit: 4, Entries: ents(5, 3)}
	if msg.String() == "" {
		t.Fatal("Message.String")
	}
	if (Entry{Index: 1, Term: 1}).String() == "" || ConfChangeAddVoter.String() != "add-voter" {
		t.Fatal("stringers")
	}
}
