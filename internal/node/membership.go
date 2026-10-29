package node

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/sadrax4/kvorum/raft"
)

// maxAddressLength bounds the addresses stored in the replicated membership
// table, which are proposed by other nodes over the Raft transport.
const maxAddressLength = 256

// Member describes a cluster member as seen by the node.
type Member struct {
	ID            raft.NodeID `json:"id"`
	RaftAddress   string      `json:"raft_address"`
	ClientAddress string      `json:"client_address,omitempty"`
	Learner       bool        `json:"learner,omitempty"`
}

// memberContext is what travels inside a ConfChange's Context field and in
// member-attribute entries: the addresses of a member.
type memberContext struct {
	RaftAddress   string `json:"raft_address,omitempty"`
	ClientAddress string `json:"client_address,omitempty"`
}

func encodeMemberContext(mc memberContext) []byte {
	b, err := json.Marshal(mc)
	if err != nil {
		panic(fmt.Sprintf("node: marshal member context: %v", err)) // cannot fail for plain strings
	}
	return b
}

func decodeMemberContext(b []byte) (memberContext, error) {
	var mc memberContext
	if len(b) == 0 {
		return mc, nil
	}
	if err := json.Unmarshal(b, &mc); err != nil {
		return mc, fmt.Errorf("decode member context: %w", err)
	}
	return mc, nil
}

// Log entry kinds. Every EntryNormal with data starts with one of these
// bytes; the leader's empty no-op entries carry no data at all.
const (
	entryKindCommand byte = 1
	entryKindAttrs   byte = 2
)

// memberAttrs is the payload of an attribute entry, used by a leader to
// publish its client address so that followers can hand out leader hints.
type memberAttrs struct {
	ID            raft.NodeID `json:"id"`
	ClientAddress string      `json:"client_address"`
}

func encodeMemberAttrs(a memberAttrs) []byte {
	b, err := json.Marshal(a)
	if err != nil {
		panic(fmt.Sprintf("node: marshal member attributes: %v", err)) // cannot fail
	}
	return b
}

func decodeMemberAttrs(b []byte) (memberAttrs, error) {
	var a memberAttrs
	if err := json.Unmarshal(b, &a); err != nil {
		return a, fmt.Errorf("decode member attributes: %w", err)
	}
	if a.ID == raft.None {
		return a, errors.New("decode member attributes: missing id")
	}
	return a, nil
}

func encodeEntry(kind byte, payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = kind
	copy(out[1:], payload)
	return out
}

// Snapshot envelope: the node stores its membership table next to the
// key-value payload so that a node restored from a snapshot knows how to
// reach its peers.
//
//	magic "KVSNAP-NODE" | version u8 | members length u32 | members JSON | store payload
var snapshotMagic = []byte("KVSNAP-NODE")

const snapshotVersion = 1

type snapshotMembers map[raft.NodeID]memberContext

func encodeSnapshot(members snapshotMembers, store []byte) ([]byte, error) {
	mb, err := json.Marshal(members)
	if err != nil {
		return nil, fmt.Errorf("encode members: %w", err)
	}
	buf := make([]byte, 0, len(snapshotMagic)+1+4+len(mb)+len(store))
	buf = append(buf, snapshotMagic...)
	buf = append(buf, snapshotVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(mb)))
	buf = append(buf, mb...)
	buf = append(buf, store...)
	return buf, nil
}

func decodeSnapshot(data []byte) (snapshotMembers, []byte, error) {
	header := len(snapshotMagic) + 1 + 4
	if len(data) < header || string(data[:len(snapshotMagic)]) != string(snapshotMagic) {
		return nil, nil, errors.New("decode snapshot: bad magic")
	}
	if v := data[len(snapshotMagic)]; v != snapshotVersion {
		return nil, nil, fmt.Errorf("decode snapshot: unsupported version %d", v)
	}
	n := binary.BigEndian.Uint32(data[len(snapshotMagic)+1:])
	rest := data[header:]
	if uint64(len(rest)) < uint64(n) {
		return nil, nil, errors.New("decode snapshot: truncated members table")
	}
	members := make(snapshotMembers)
	if n > 0 {
		if err := json.Unmarshal(rest[:n], &members); err != nil {
			return nil, nil, fmt.Errorf("decode snapshot members: %w", err)
		}
	}
	return members, rest[n:], nil
}

// membersLocked renders the membership table merged with the raft
// configuration. Callers hold n.mu.
func (n *Node) membersLocked() []Member {
	out := make([]Member, 0, len(n.confState.Voters)+len(n.confState.Learners))
	add := func(id raft.NodeID, learner bool) {
		mc := n.members[id]
		out = append(out, Member{ID: id, RaftAddress: mc.RaftAddress, ClientAddress: mc.ClientAddress, Learner: learner})
	}
	for _, id := range n.confState.Voters {
		add(id, false)
	}
	for _, id := range n.confState.Learners {
		add(id, true)
	}
	slices.SortFunc(out, func(a, b Member) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return out
}

// Members returns the current cluster membership.
func (n *Node) Members() []Member {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.membersLocked()
}

// Leader returns the current leader and what is known about it. ok is false
// when no leader is known.
func (n *Node) Leader() (Member, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.leader == raft.None {
		return Member{}, false
	}
	mc := n.members[n.leader]
	return Member{
		ID:            n.leader,
		RaftAddress:   mc.RaftAddress,
		ClientAddress: mc.ClientAddress,
		Learner:       n.confState.IsLearner(n.leader),
	}, true
}
