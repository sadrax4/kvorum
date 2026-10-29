package raft

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
)

// NodeID uniquely identifies a member of a Raft cluster. The zero value,
// [None], is reserved and never names a real node.
type NodeID uint64

// None is the NodeID used when no node is referenced: no known leader, no vote
// cast in the current term, and so on.
const None NodeID = 0

// String implements fmt.Stringer.
func (id NodeID) String() string {
	if id == None {
		return "none"
	}
	return strconv.FormatUint(uint64(id), 10)
}

// Term is a Raft term number. Terms increase monotonically and act as a
// logical clock across the cluster.
type Term uint64

// Index is the position of an entry in the replicated log. The log is
// 1-indexed; index 0 denotes the empty log (or the position of a snapshot
// that compacted everything).
type Index uint64

// StateType is the role a node currently plays.
type StateType uint8

const (
	// StateFollower nodes accept entries from a leader and grant votes.
	StateFollower StateType = iota
	// StatePreCandidate nodes are probing whether an election could succeed
	// before incrementing their term (Pre-Vote).
	StatePreCandidate
	// StateCandidate nodes have incremented their term and are requesting
	// votes.
	StateCandidate
	// StateLeader nodes replicate entries to all other members.
	StateLeader
)

var stateNames = [...]string{"follower", "pre-candidate", "candidate", "leader"}

// String implements fmt.Stringer.
func (s StateType) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return "unknown(" + strconv.Itoa(int(s)) + ")"
}

// EntryType distinguishes ordinary commands from cluster configuration
// changes in the replicated log.
type EntryType uint8

const (
	// EntryNormal carries an opaque application command in Data.
	EntryNormal EntryType = iota
	// EntryConfChange carries a serialized [ConfChange] in Data.
	EntryConfChange
)

// String implements fmt.Stringer.
func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryConfChange:
		return "conf-change"
	default:
		return "unknown(" + strconv.Itoa(int(t)) + ")"
	}
}

// Entry is a single record in the replicated log.
type Entry struct {
	Index Index
	Term  Term
	Type  EntryType
	Data  []byte
}

// entryHeaderSize is the number of bytes the fixed fields of an entry occupy
// in the compact binary encoding used by the WAL and transport. It is used to
// estimate message sizes for batching.
const entryHeaderSize = 8 + 8 + 1

// Size returns the approximate encoded size of the entry in bytes. It is used
// to honour [Config.MaxSizePerMsg] and [Config.MaxCommittedSizePerReady].
func (e Entry) Size() uint64 {
	return uint64(entryHeaderSize + len(e.Data))
}

// String implements fmt.Stringer.
func (e Entry) String() string {
	return fmt.Sprintf("{%d/%d %s %dB}", e.Term, e.Index, e.Type, len(e.Data))
}

// MessageType identifies the Raft RPC a [Message] represents.
type MessageType uint8

const (
	// MsgPreVote asks whether a peer would grant a vote for Term without the
	// peer actually incrementing its term.
	MsgPreVote MessageType = iota + 1
	// MsgPreVoteResp answers a MsgPreVote.
	MsgPreVoteResp
	// MsgVote is the RequestVote RPC.
	MsgVote
	// MsgVoteResp answers a MsgVote.
	MsgVoteResp
	// MsgApp is the AppendEntries RPC used for replication.
	MsgApp
	// MsgAppResp answers a MsgApp, and also acknowledges a MsgSnap.
	MsgAppResp
	// MsgHeartbeat is an empty AppendEntries used to maintain leadership and to
	// confirm quorum for ReadIndex.
	MsgHeartbeat
	// MsgHeartbeatResp answers a MsgHeartbeat.
	MsgHeartbeatResp
	// MsgSnap is the InstallSnapshot RPC.
	MsgSnap
	// MsgTimeoutNow instructs the recipient to start an election immediately
	// as part of a leadership transfer.
	MsgTimeoutNow
	// MsgReadIndex is sent by a follower to the leader to obtain a read index.
	MsgReadIndex
	// MsgReadIndexResp returns the read index to a follower.
	MsgReadIndexResp
)

var messageNames = [...]string{
	"invalid",
	"MsgPreVote", "MsgPreVoteResp",
	"MsgVote", "MsgVoteResp",
	"MsgApp", "MsgAppResp",
	"MsgHeartbeat", "MsgHeartbeatResp",
	"MsgSnap",
	"MsgTimeoutNow",
	"MsgReadIndex", "MsgReadIndexResp",
}

// String implements fmt.Stringer.
func (t MessageType) String() string {
	if int(t) < len(messageNames) {
		return messageNames[t]
	}
	return "MsgUnknown(" + strconv.Itoa(int(t)) + ")"
}

// IsResponse reports whether the message type is a reply to another message.
func (t MessageType) IsResponse() bool {
	switch t {
	case MsgPreVoteResp, MsgVoteResp, MsgAppResp, MsgHeartbeatResp, MsgReadIndexResp:
		return true
	default:
		return false
	}
}

// Message is a Raft RPC request or response. A single struct is used for all
// message types so that transports can encode it uniformly; which fields are
// meaningful depends on Type, as documented per field.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	// Term is the sender's current term, except for MsgPreVote, where it is the
	// term the sender would campaign in (current term + 1).
	Term Term

	// LastLogIndex and LastLogTerm describe the candidate's last entry in
	// MsgPreVote and MsgVote.
	LastLogIndex Index
	LastLogTerm  Term

	// PrevLogIndex and PrevLogTerm identify the entry immediately preceding
	// Entries in MsgApp. A rejected MsgAppResp echoes PrevLogIndex so the
	// leader can detect stale rejections.
	PrevLogIndex Index
	PrevLogTerm  Term
	// Entries are the log entries carried by MsgApp.
	Entries []Entry
	// Commit is the leader's commit index in MsgApp and MsgHeartbeat (bounded
	// by what the recipient is known to have).
	Commit Index

	// Reject is set on vote responses to deny the vote and on MsgAppResp to
	// signal a log mismatch.
	Reject bool
	// MatchIndex is the last log index that matches the leader's log, set on
	// an accepting MsgAppResp.
	MatchIndex Index
	// RejectHint is the follower's last log index on a rejecting MsgAppResp.
	RejectHint Index
	// ConflictTerm is the term of the follower's entry at PrevLogIndex when it
	// disagrees with PrevLogTerm, or zero if the follower has no such entry.
	ConflictTerm Term
	// ConflictIndex is the first index the follower holds for ConflictTerm.
	ConflictIndex Index

	// Snapshot carries the snapshot for MsgSnap.
	Snapshot *Snapshot

	// Context is an opaque correlation token used by MsgReadIndex and
	// piggybacked on heartbeats while the leader confirms its quorum.
	Context []byte
	// ReadIndex is the commit index the leader observed when it received the
	// corresponding MsgReadIndex; set on MsgReadIndexResp.
	ReadIndex Index

	// Transfer marks a MsgVote issued in response to MsgTimeoutNow. Peers grant
	// such votes even if they recently heard from a leader.
	Transfer bool
}

// String renders a compact human-readable description of the message.
func (m Message) String() string {
	s := fmt.Sprintf("%s %s->%s term=%d", m.Type, m.From, m.To, m.Term)
	switch m.Type {
	case MsgPreVote, MsgVote:
		s += fmt.Sprintf(" last=%d/%d", m.LastLogTerm, m.LastLogIndex)
		if m.Transfer {
			s += " transfer"
		}
	case MsgPreVoteResp, MsgVoteResp:
		s += fmt.Sprintf(" reject=%t", m.Reject)
	case MsgApp:
		s += fmt.Sprintf(" prev=%d/%d commit=%d entries=%d", m.PrevLogTerm, m.PrevLogIndex, m.Commit, len(m.Entries))
	case MsgAppResp:
		if m.Reject {
			s += fmt.Sprintf(" reject prev=%d hint=%d conflict=%d/%d", m.PrevLogIndex, m.RejectHint, m.ConflictTerm, m.ConflictIndex)
		} else {
			s += fmt.Sprintf(" match=%d", m.MatchIndex)
		}
	case MsgHeartbeat:
		s += fmt.Sprintf(" commit=%d", m.Commit)
	case MsgSnap:
		if m.Snapshot != nil {
			s += fmt.Sprintf(" snapshot=%d/%d", m.Snapshot.Meta.Term, m.Snapshot.Meta.Index)
		}
	case MsgReadIndexResp:
		s += fmt.Sprintf(" readIndex=%d", m.ReadIndex)
	}
	return s
}

// HardState is the state that must be durably stored before a node sends any
// message that depends on it. Losing it can violate safety: a node that
// forgets its vote could vote twice in a term, and a node that forgets its
// term could accept stale leaders.
type HardState struct {
	Term   Term
	Vote   NodeID
	Commit Index
}

// IsEmpty reports whether the hard state is the zero value, which in a
// [Ready] signals that nothing changed.
func (hs HardState) IsEmpty() bool {
	return hs == HardState{}
}

// SoftState is volatile state that is useful to the application (for example
// to redirect clients) but never needs to be persisted.
type SoftState struct {
	Leader NodeID
	State  StateType
}

// ConfState is the cluster membership at a point in the log. Voters take part
// in elections and commitment; learners receive the log but do not count
// towards any quorum.
type ConfState struct {
	Voters   []NodeID
	Learners []NodeID
}

// IsEmpty reports whether the configuration has no members at all.
func (cs ConfState) IsEmpty() bool {
	return len(cs.Voters) == 0 && len(cs.Learners) == 0
}

// IsVoter reports whether id is a voting member.
func (cs ConfState) IsVoter(id NodeID) bool {
	return slices.Contains(cs.Voters, id)
}

// IsLearner reports whether id is a learner.
func (cs ConfState) IsLearner(id NodeID) bool {
	return slices.Contains(cs.Learners, id)
}

// Contains reports whether id is a member of any kind.
func (cs ConfState) Contains(id NodeID) bool {
	return cs.IsVoter(id) || cs.IsLearner(id)
}

// Clone returns a deep copy with sorted member lists.
func (cs ConfState) Clone() ConfState {
	out := ConfState{
		Voters:   slices.Clone(cs.Voters),
		Learners: slices.Clone(cs.Learners),
	}
	slices.Sort(out.Voters)
	slices.Sort(out.Learners)
	return out
}

// Equal reports whether two configurations have the same members, ignoring
// order.
func (cs ConfState) Equal(other ConfState) bool {
	a, b := cs.Clone(), other.Clone()
	return slices.Equal(a.Voters, b.Voters) && slices.Equal(a.Learners, b.Learners)
}

// String implements fmt.Stringer.
func (cs ConfState) String() string {
	c := cs.Clone()
	return fmt.Sprintf("{voters=%v learners=%v}", c.Voters, c.Learners)
}

// ConfChangeType is the kind of membership change a [ConfChange] requests.
type ConfChangeType uint8

const (
	// ConfChangeAddVoter adds a new voting member, or promotes an existing
	// learner to voter.
	ConfChangeAddVoter ConfChangeType = iota + 1
	// ConfChangeAddLearner adds a non-voting member that receives the log.
	ConfChangeAddLearner
	// ConfChangeRemoveNode removes a voter or learner.
	ConfChangeRemoveNode
)

// String implements fmt.Stringer.
func (t ConfChangeType) String() string {
	switch t {
	case ConfChangeAddVoter:
		return "add-voter"
	case ConfChangeAddLearner:
		return "add-learner"
	case ConfChangeRemoveNode:
		return "remove-node"
	default:
		return "unknown(" + strconv.Itoa(int(t)) + ")"
	}
}

// ConfChange is a single-server membership change. Context is opaque to the
// library; applications typically use it to carry the member's network
// address.
type ConfChange struct {
	Type    ConfChangeType
	NodeID  NodeID
	Context []byte
}

// SnapshotMeta describes the log position and membership captured by a
// snapshot.
type SnapshotMeta struct {
	// Index is the last log index included in the snapshot.
	Index Index
	// Term is the term of the entry at Index.
	Term Term
	// ConfState is the membership as of Index.
	ConfState ConfState
}

// Snapshot is a point-in-time copy of the application state machine, used
// both for log compaction and to bring far-behind followers up to date.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// IsEmpty reports whether the snapshot is the zero value.
func (s Snapshot) IsEmpty() bool {
	return s.Meta.Index == 0
}

// ReadState is delivered in a [Ready] once a ReadIndex request has been
// confirmed by a quorum. The application must wait until it has applied at
// least Index before serving the read associated with Context.
type ReadState struct {
	Index   Index
	Context []byte
}

// Ready is the unit of work the application must process after stepping the
// node. See the package documentation for the ordering requirements.
type Ready struct {
	// SoftState is non-nil when the leader or the node's role changed.
	SoftState *SoftState

	// HardState is the updated durable state, or the zero value if unchanged.
	// It must be persisted before Messages are sent.
	HardState HardState

	// Entries must be appended to stable storage before Messages are sent.
	Entries []Entry

	// Snapshot, when non-empty, must be persisted and the application state
	// machine must be restored from it before CommittedEntries are applied.
	Snapshot Snapshot

	// CommittedEntries are entries ready to be applied to the state machine,
	// in order. Configuration changes among them must be passed to
	// [RawNode.ApplyConfChange].
	CommittedEntries []Entry

	// Messages must be sent to their destinations after HardState, Entries,
	// and Snapshot have been persisted.
	Messages []Message

	// ReadStates are read requests that have been confirmed by a quorum.
	ReadStates []ReadState

	// MustSync indicates whether the HardState or Entries carry information
	// that must reach stable storage (fsync) before Messages are sent. When
	// false the application may defer the sync, for example because only the
	// commit index changed.
	MustSync bool
}

// Config holds the parameters used to start a [RawNode].
type Config struct {
	// ID is the identity of the local node. Must not be None.
	ID NodeID

	// ElectionTick is the number of Tick calls that must elapse without
	// hearing from a leader before a follower starts an election. The actual
	// timeout is randomized in [ElectionTick, 2*ElectionTick) to reduce split
	// votes. Must be greater than HeartbeatTick; ten times is a good default.
	ElectionTick int

	// HeartbeatTick is the number of Tick calls between leader heartbeats.
	HeartbeatTick int

	// Storage provides the persisted log, hard state, and snapshot. The
	// application writes to it in response to Ready; the library only reads.
	Storage Storage

	// Applied is the index of the last entry the application has applied. It
	// must be set on restart so that already-applied entries are not
	// delivered again.
	Applied Index

	// MaxSizePerMsg caps the total byte size of entries in a single MsgApp.
	// Zero means a sensible default.
	MaxSizePerMsg uint64

	// MaxInflightMsgs caps the number of MsgApp messages in flight to a single
	// follower during optimistic replication. Zero means a sensible default.
	MaxInflightMsgs int

	// MaxCommittedSizePerReady caps the byte size of CommittedEntries in a
	// single Ready, bounding apply latency. Zero means a sensible default.
	MaxCommittedSizePerReady uint64

	// PreVote enables the Pre-Vote extension. Strongly recommended.
	PreVote bool

	// CheckQuorum makes a leader step down when it has not heard from a quorum
	// of followers within an election timeout, and makes followers ignore
	// vote requests while they are in contact with a leader. Required for
	// ReadIndex to be safe under clock-free assumptions.
	CheckQuorum bool

	// Logger receives diagnostic output. Nil disables logging.
	Logger *slog.Logger

	// Rand is the source of election-timeout randomness. Nil uses a
	// non-deterministic source; tests inject a seeded source for
	// reproducibility.
	Rand *rand.Rand
}

const (
	defaultMaxSizePerMsg            = 1 << 20 // 1 MiB
	defaultMaxInflightMsgs          = 256
	defaultMaxCommittedSizePerReady = 64 << 20 // 64 MiB
)

func (c *Config) validate() error {
	if c.ID == None {
		return fmt.Errorf("%w: ID must not be zero", ErrInvalidConfig)
	}
	if c.HeartbeatTick <= 0 {
		return fmt.Errorf("%w: HeartbeatTick must be greater than zero", ErrInvalidConfig)
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return fmt.Errorf("%w: ElectionTick must be greater than HeartbeatTick", ErrInvalidConfig)
	}
	if c.Storage == nil {
		return fmt.Errorf("%w: Storage must not be nil", ErrInvalidConfig)
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = defaultMaxSizePerMsg
	}
	if c.MaxInflightMsgs <= 0 {
		c.MaxInflightMsgs = defaultMaxInflightMsgs
	}
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = defaultMaxCommittedSizePerReady
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Rand == nil {
		// Election jitter only needs to be unpredictable enough to avoid
		// synchronized timeouts; it is not security sensitive.
		c.Rand = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // see above
	}
	return nil
}

// ProgressStateType describes how the leader is currently replicating to a
// particular follower.
type ProgressStateType uint8

const (
	// ProgressProbe sends one message at a time until the follower's log
	// position is known.
	ProgressProbe ProgressStateType = iota
	// ProgressReplicate streams entries optimistically with a bounded window.
	ProgressReplicate
	// ProgressSnapshot waits for a snapshot to be installed before resuming.
	ProgressSnapshot
)

// String implements fmt.Stringer.
func (s ProgressStateType) String() string {
	switch s {
	case ProgressProbe:
		return "probe"
	case ProgressReplicate:
		return "replicate"
	case ProgressSnapshot:
		return "snapshot"
	default:
		return "unknown(" + strconv.Itoa(int(s)) + ")"
	}
}

// ProgressStatus is a read-only view of a leader's replication progress for
// one follower.
type ProgressStatus struct {
	Match        Index
	Next         Index
	State        ProgressStateType
	RecentActive bool
	IsLearner    bool
}

// Status is a point-in-time snapshot of the node's state for monitoring.
type Status struct {
	ID        NodeID
	State     StateType
	Term      Term
	Vote      NodeID
	Leader    NodeID
	Commit    Index
	Applied   Index
	LastIndex Index
	ConfState ConfState
	// Progress is populated only on the leader.
	Progress map[NodeID]ProgressStatus
}
