package client

import (
	"errors"

	"github.com/sadrax4/kvorum/api/kvorumpb"
)

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("client: key not found")
	// ErrNoEndpoints is returned by New when the configuration names no
	// endpoint.
	ErrNoEndpoints = errors.New("client: no endpoints configured")
	// ErrClosed is returned by every method once Close has been called.
	ErrClosed = errors.New("client: client is closed")
)

// KeyValue is a key together with its value and version metadata.
type KeyValue struct {
	// Key is the key.
	Key []byte
	// Value is the value stored at Key.
	Value []byte
	// Version counts the writes to the key since it was created; the first
	// write has version 1. Deleting the key resets it.
	Version uint64
	// CreateRevision is the revision at which the key was created.
	CreateRevision uint64
	// ModRevision is the revision of the last write to the key.
	ModRevision uint64
}

// Header carries the metadata attached to every response.
type Header struct {
	// MemberID is the id of the node that served the request.
	MemberID uint64
	// Term is the Raft term in which the request was served.
	Term uint64
	// Revision is the Raft log index of the last entry applied by the serving
	// node when the response was produced.
	Revision uint64
}

// PutResult is the outcome of a Put.
type PutResult struct {
	// Header is the response header.
	Header Header
	// KV is the key as stored after the write.
	KV *KeyValue
	// PrevKV is the key as it was before the write. It is set only when the
	// write was issued WithPrevKV and the key existed.
	PrevKV *KeyValue
}

// DeleteResult is the outcome of a Delete.
type DeleteResult struct {
	// Header is the response header.
	Header Header
	// Deleted is false when the key did not exist.
	Deleted bool
	// PrevKV is the key as it was before the delete. It is set only when the
	// delete was issued WithPrevKV and the key existed.
	PrevKV *KeyValue
}

// CASResult is the outcome of a CompareAndSwap.
type CASResult struct {
	// Header is the response header.
	Header Header
	// Succeeded reports whether the expected version matched and the value
	// was written.
	Succeeded bool
	// KV is the key as stored after the call: the new value on success, the
	// current value on failure. It is nil when the key does not exist.
	KV *KeyValue
}

// ScanResult is the outcome of a Scan.
type ScanResult struct {
	// Header is the response header.
	Header Header
	// KVs holds the keys of the range in ascending byte order.
	KVs []KeyValue
	// More is true when the limit cut the result short.
	More bool
}

// EventType is the kind of change a WatchEvent describes.
type EventType uint8

const (
	// EventUnspecified is the zero value; it is never produced by a server
	// that follows the protocol.
	EventUnspecified EventType = iota
	// EventPut reports that a key was created or overwritten.
	EventPut
	// EventDelete reports that a key was removed.
	EventDelete
)

// String implements fmt.Stringer.
func (t EventType) String() string {
	switch t {
	case EventPut:
		return "PUT"
	case EventDelete:
		return "DELETE"
	default:
		return "UNSPECIFIED"
	}
}

// WatchEvent is one change delivered by Watch, or the terminal error of the
// watch when Err is set.
type WatchEvent struct {
	// Type is the kind of change.
	Type EventType
	// KV is the key after the change. For deletes only Key and ModRevision
	// are set.
	KV KeyValue
	// PrevKV is the key before the change, when the server reports it.
	PrevKV *KeyValue
	// Header is the header of the stream message that carried the event.
	Header Header
	// Err is set on the last event of a watch that ended because of an
	// unrecoverable error or because its context ended. The channel is closed
	// right after such an event.
	Err error
}

// Member describes one node of the cluster.
type Member struct {
	// ID is the Raft node id.
	ID uint64
	// RaftAddress is the address peers use for Raft traffic.
	RaftAddress string
	// ClientAddress is the address clients use for the KV API.
	ClientAddress string
	// Learner is true for members that replicate the log without voting.
	Learner bool
}

// Status reports the Raft state of the node that served the request.
type Status struct {
	// Header is the response header.
	Header Header
	// ID is the node id.
	ID uint64
	// State is one of follower, pre-candidate, candidate, leader.
	State string
	// Term is the current Raft term.
	Term uint64
	// Leader is the id of the current leader, or zero when unknown.
	Leader uint64
	// CommitIndex is the highest log index known to be committed.
	CommitIndex uint64
	// AppliedIndex is the highest log index applied to the state machine.
	AppliedIndex uint64
	// LastIndex is the highest log index stored by the node.
	LastIndex uint64
	// SnapshotIndex is the index of the latest snapshot.
	SnapshotIndex uint64
	// Members is the current membership.
	Members []Member
	// Version is the server build version.
	Version string
}

func headerFromProto(h *kvorumpb.ResponseHeader) Header {
	if h == nil {
		return Header{}
	}
	return Header{MemberID: h.GetMemberId(), Term: h.GetTerm(), Revision: h.GetRevision()}
}

func kvFromProto(kv *kvorumpb.KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	return &KeyValue{
		Key:            kv.GetKey(),
		Value:          kv.GetValue(),
		Version:        kv.GetVersion(),
		CreateRevision: kv.GetCreateRevision(),
		ModRevision:    kv.GetModRevision(),
	}
}

func kvsFromProto(kvs []*kvorumpb.KeyValue) []KeyValue {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		out = append(out, *kvFromProto(kv))
	}
	return out
}

func memberFromProto(m *kvorumpb.Member) Member {
	if m == nil {
		return Member{}
	}
	return Member{
		ID:            m.GetId(),
		RaftAddress:   m.GetRaftAddress(),
		ClientAddress: m.GetClientAddress(),
		Learner:       m.GetLearner(),
	}
}

func membersFromProto(ms []*kvorumpb.Member) []Member {
	if len(ms) == 0 {
		return nil
	}
	out := make([]Member, 0, len(ms))
	for _, m := range ms {
		out = append(out, memberFromProto(m))
	}
	return out
}

func memberToProto(m Member) *kvorumpb.Member {
	return &kvorumpb.Member{
		Id:            m.ID,
		RaftAddress:   m.RaftAddress,
		ClientAddress: m.ClientAddress,
		Learner:       m.Learner,
	}
}

func eventTypeFromProto(t kvorumpb.Event_Type) EventType {
	switch t {
	case kvorumpb.Event_TYPE_PUT:
		return EventPut
	case kvorumpb.Event_TYPE_DELETE:
		return EventDelete
	default:
		return EventUnspecified
	}
}
