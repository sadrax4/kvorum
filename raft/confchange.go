package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Binary encodings for ConfChange and ConfState. They are deliberately simple
// (fixed-width big-endian integers) so that the WAL, snapshots, and
// transports can store them without depending on protobuf.

var errShortBuffer = errors.New("raft: buffer too short")

// Marshal encodes the ConfChange for storage in an Entry.
func (cc ConfChange) Marshal() []byte {
	buf := make([]byte, 0, 1+8+4+len(cc.Context))
	buf = append(buf, byte(cc.Type))
	buf = binary.BigEndian.AppendUint64(buf, uint64(cc.NodeID))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(cc.Context)))
	buf = append(buf, cc.Context...)
	return buf
}

// UnmarshalConfChange decodes a ConfChange produced by [ConfChange.Marshal].
func UnmarshalConfChange(data []byte) (ConfChange, error) {
	if len(data) < 1+8+4 {
		return ConfChange{}, fmt.Errorf("conf change: %w", errShortBuffer)
	}
	cc := ConfChange{
		Type:   ConfChangeType(data[0]),
		NodeID: NodeID(binary.BigEndian.Uint64(data[1:9])),
	}
	n := binary.BigEndian.Uint32(data[9:13])
	rest := data[13:]
	if uint64(len(rest)) != uint64(n) {
		return ConfChange{}, fmt.Errorf("conf change: context length %d does not match payload %d", n, len(rest))
	}
	switch cc.Type {
	case ConfChangeAddVoter, ConfChangeAddLearner, ConfChangeRemoveNode:
	default:
		return ConfChange{}, fmt.Errorf("conf change: unknown type %d", cc.Type)
	}
	if cc.NodeID == None {
		return ConfChange{}, errors.New("conf change: node id must not be zero")
	}
	if n > 0 {
		cc.Context = append([]byte(nil), rest...)
	}
	return cc, nil
}

// String implements fmt.Stringer.
func (cc ConfChange) String() string {
	return fmt.Sprintf("{%s node=%d ctx=%dB}", cc.Type, cc.NodeID, len(cc.Context))
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (cs ConfState) MarshalBinary() ([]byte, error) {
	c := cs.Clone()
	buf := make([]byte, 0, 8+8*(len(c.Voters)+len(c.Learners)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Voters)))
	for _, id := range c.Voters {
		buf = binary.BigEndian.AppendUint64(buf, uint64(id))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Learners)))
	for _, id := range c.Learners {
		buf = binary.BigEndian.AppendUint64(buf, uint64(id))
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (cs *ConfState) UnmarshalBinary(data []byte) error {
	readIDs := func() ([]NodeID, error) {
		if len(data) < 4 {
			return nil, errShortBuffer
		}
		n := binary.BigEndian.Uint32(data)
		data = data[4:]
		if uint64(len(data)) < uint64(n)*8 {
			return nil, errShortBuffer
		}
		ids := make([]NodeID, 0, n)
		for range n {
			ids = append(ids, NodeID(binary.BigEndian.Uint64(data)))
			data = data[8:]
		}
		return ids, nil
	}
	voters, err := readIDs()
	if err != nil {
		return fmt.Errorf("conf state voters: %w", err)
	}
	learners, err := readIDs()
	if err != nil {
		return fmt.Errorf("conf state learners: %w", err)
	}
	if len(data) != 0 {
		return fmt.Errorf("conf state: %d trailing bytes", len(data))
	}
	*cs = ConfState{Voters: voters, Learners: learners}
	return nil
}

// apply returns the membership that results from applying cc to cs. It is
// pure: cs is not modified.
func (cs ConfState) apply(cc ConfChange) ConfState {
	out := cs.Clone()
	remove := func(ids []NodeID, id NodeID) []NodeID {
		res := ids[:0:0]
		for _, x := range ids {
			if x != id {
				res = append(res, x)
			}
		}
		return res
	}
	switch cc.Type {
	case ConfChangeAddVoter:
		out.Learners = remove(out.Learners, cc.NodeID)
		if !out.IsVoter(cc.NodeID) {
			out.Voters = append(out.Voters, cc.NodeID)
		}
	case ConfChangeAddLearner:
		if !out.IsVoter(cc.NodeID) && !out.IsLearner(cc.NodeID) {
			out.Learners = append(out.Learners, cc.NodeID)
		}
	case ConfChangeRemoveNode:
		out.Voters = remove(out.Voters, cc.NodeID)
		out.Learners = remove(out.Learners, cc.NodeID)
	}
	return out.Clone()
}
