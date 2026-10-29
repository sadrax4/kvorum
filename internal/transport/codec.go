package transport

import (
	"fmt"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

// ToProto converts m to its wire representation. For MsgSnap only the
// snapshot metadata is encoded: the payload travels in InstallSnapshot data
// chunks. Byte slices are shared with m, not copied; the result must be
// marshalled before m is modified.
func ToProto(m raft.Message) *raftpb.Message {
	pm := &raftpb.Message{
		Type:          uint32(m.Type),
		From:          uint64(m.From),
		To:            uint64(m.To),
		Term:          uint64(m.Term),
		LastLogIndex:  uint64(m.LastLogIndex),
		LastLogTerm:   uint64(m.LastLogTerm),
		PrevLogIndex:  uint64(m.PrevLogIndex),
		PrevLogTerm:   uint64(m.PrevLogTerm),
		Commit:        uint64(m.Commit),
		Reject:        m.Reject,
		MatchIndex:    uint64(m.MatchIndex),
		RejectHint:    uint64(m.RejectHint),
		ConflictTerm:  uint64(m.ConflictTerm),
		ConflictIndex: uint64(m.ConflictIndex),
		Context:       m.Context,
		ReadIndex:     uint64(m.ReadIndex),
		Transfer:      m.Transfer,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*raftpb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = &raftpb.Entry{
				Index: uint64(e.Index),
				Term:  uint64(e.Term),
				Type:  uint32(e.Type),
				Data:  e.Data,
			}
		}
	}
	if m.Type == raft.MsgSnap && m.Snapshot != nil {
		pm.SnapshotMeta = snapshotMetaToProto(m.Snapshot.Meta)
	}
	return pm
}

func snapshotMetaToProto(meta raft.SnapshotMeta) *raftpb.SnapshotMeta {
	return &raftpb.SnapshotMeta{
		Index: uint64(meta.Index),
		Term:  uint64(meta.Term),
		ConfState: &raftpb.ConfState{
			Voters:   nodeIDsToProto(meta.ConfState.Voters),
			Learners: nodeIDsToProto(meta.ConfState.Learners),
		},
	}
}

func nodeIDsToProto(ids []raft.NodeID) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

// FromProto converts a wire message into a [raft.Message], validating every
// enumeration, pointer, and the presence of snapshot metadata so that a
// malformed peer cannot produce an unusable message. For MsgSnap the result
// carries a Snapshot with Meta set and empty Data; the server fills Data from
// the stream. Errors wrap [ErrInvalidMessage]. Byte slices are shared with
// pm, not copied.
func FromProto(pm *raftpb.Message) (raft.Message, error) {
	if pm == nil {
		return raft.Message{}, fmt.Errorf("%w: nil message", ErrInvalidMessage)
	}
	mt, err := messageTypeFromProto(pm.GetType())
	if err != nil {
		return raft.Message{}, err
	}
	m := raft.Message{
		Type:          mt,
		From:          raft.NodeID(pm.GetFrom()),
		To:            raft.NodeID(pm.GetTo()),
		Term:          raft.Term(pm.GetTerm()),
		LastLogIndex:  raft.Index(pm.GetLastLogIndex()),
		LastLogTerm:   raft.Term(pm.GetLastLogTerm()),
		PrevLogIndex:  raft.Index(pm.GetPrevLogIndex()),
		PrevLogTerm:   raft.Term(pm.GetPrevLogTerm()),
		Commit:        raft.Index(pm.GetCommit()),
		Reject:        pm.GetReject(),
		MatchIndex:    raft.Index(pm.GetMatchIndex()),
		RejectHint:    raft.Index(pm.GetRejectHint()),
		ConflictTerm:  raft.Term(pm.GetConflictTerm()),
		ConflictIndex: raft.Index(pm.GetConflictIndex()),
		Context:       nonEmpty(pm.GetContext()),
		ReadIndex:     raft.Index(pm.GetReadIndex()),
		Transfer:      pm.GetTransfer(),
	}
	if pes := pm.GetEntries(); len(pes) > 0 {
		m.Entries = make([]raft.Entry, len(pes))
		for i, pe := range pes {
			e, err := entryFromProto(i, pe)
			if err != nil {
				return raft.Message{}, err
			}
			m.Entries[i] = e
		}
	}
	switch meta := pm.GetSnapshotMeta(); {
	case mt == raft.MsgSnap && meta == nil:
		return raft.Message{}, fmt.Errorf("%w: MsgSnap without snapshot metadata", ErrInvalidMessage)
	case mt == raft.MsgSnap:
		sm, err := snapshotMetaFromProto(meta)
		if err != nil {
			return raft.Message{}, err
		}
		m.Snapshot = &raft.Snapshot{Meta: sm}
	case meta != nil:
		return raft.Message{}, fmt.Errorf("%w: snapshot metadata on %s", ErrInvalidMessage, mt)
	}
	return m, nil
}

func messageTypeFromProto(v uint32) (raft.MessageType, error) {
	if v < uint32(raft.MsgPreVote) || v > uint32(raft.MsgReadIndexResp) {
		return 0, fmt.Errorf("%w: message type %d out of range", ErrInvalidMessage, v)
	}
	return raft.MessageType(v), nil
}

func entryFromProto(i int, pe *raftpb.Entry) (raft.Entry, error) {
	if pe == nil {
		return raft.Entry{}, fmt.Errorf("%w: entry %d is nil", ErrInvalidMessage, i)
	}
	if et := pe.GetType(); et > uint32(raft.EntryConfChange) {
		return raft.Entry{}, fmt.Errorf("%w: entry %d has type %d out of range", ErrInvalidMessage, i, et)
	}
	return raft.Entry{
		Index: raft.Index(pe.GetIndex()),
		Term:  raft.Term(pe.GetTerm()),
		Type:  raft.EntryType(pe.GetType()),
		Data:  nonEmpty(pe.GetData()),
	}, nil
}

func snapshotMetaFromProto(pm *raftpb.SnapshotMeta) (raft.SnapshotMeta, error) {
	meta := raft.SnapshotMeta{
		Index: raft.Index(pm.GetIndex()),
		Term:  raft.Term(pm.GetTerm()),
	}
	cs := pm.GetConfState()
	voters, err := nodeIDsFromProto("voter", cs.GetVoters())
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	learners, err := nodeIDsFromProto("learner", cs.GetLearners())
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	meta.ConfState = raft.ConfState{Voters: voters, Learners: learners}
	return meta, nil
}

func nodeIDsFromProto(role string, ids []uint64) ([]raft.NodeID, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]raft.NodeID, len(ids))
	for i, id := range ids {
		if id == uint64(raft.None) {
			return nil, fmt.Errorf("%w: %s %d is the zero node id", ErrInvalidMessage, role, i)
		}
		out[i] = raft.NodeID(id)
	}
	return out, nil
}

// nonEmpty normalises an empty slice to nil so that decoded messages compare
// equal to the originals regardless of how the wire format represents
// absence.
func nonEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
