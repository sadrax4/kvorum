package raft

import "errors"

var (
	// ErrNotLeader is returned when an operation that requires leadership is
	// attempted on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")

	// ErrProposalDropped is returned when a proposal cannot be accepted, for
	// example because the node is not the leader, a leadership transfer is in
	// progress, or the node has been removed from the cluster.
	ErrProposalDropped = errors.New("raft: proposal dropped")

	// ErrConfChangePending is returned when a configuration change is proposed
	// while an earlier one has not yet been applied. Only one change may be in
	// flight at a time; see docs/adr/0003-single-server-membership-changes.md.
	ErrConfChangePending = errors.New("raft: configuration change already pending")

	// ErrLeaderTermNotCommitted is returned when a configuration change is
	// proposed before the leader has committed an entry from its current term.
	ErrLeaderTermNotCommitted = errors.New("raft: leader has not committed an entry in its term")

	// ErrCompacted is returned by Storage when the requested index has been
	// discarded by log compaction.
	ErrCompacted = errors.New("raft: requested index is unavailable due to compaction")

	// ErrUnavailable is returned by Storage when the requested index is beyond
	// the last stored entry.
	ErrUnavailable = errors.New("raft: requested entry is unavailable")

	// ErrSnapshotOutOfDate is returned when an operation refers to a snapshot
	// older than the one already held.
	ErrSnapshotOutOfDate = errors.New("raft: snapshot is out of date")

	// ErrSnapshotTemporarilyUnavailable can be returned by Storage.Snapshot when
	// a snapshot is being generated; the leader will retry later.
	ErrSnapshotTemporarilyUnavailable = errors.New("raft: snapshot is temporarily unavailable")

	// ErrUnknownNode is returned when a message or request references a node
	// that is not part of the configuration.
	ErrUnknownNode = errors.New("raft: unknown node")

	// ErrLearnerCannotCampaign is returned when a learner attempts to start an
	// election. Learners replicate the log but never vote or lead.
	ErrLearnerCannotCampaign = errors.New("raft: learners cannot campaign")

	// ErrStepLocalMsg is returned when Step receives a message type that must
	// originate from the local node rather than a peer.
	ErrStepLocalMsg = errors.New("raft: cannot step a local message")

	// ErrStepPeerNotFound is returned when a response is received from a peer
	// the leader does not track.
	ErrStepPeerNotFound = errors.New("raft: response from unknown peer")

	// ErrInvalidConfig is returned by NewRawNode when the Config is malformed.
	ErrInvalidConfig = errors.New("raft: invalid config")
)

// ErrInvalidConfChange is returned when a proposed configuration change does
// not make sense against the current membership, for example removing a node
// that is not a member or removing the last voter.
var ErrInvalidConfChange = errors.New("raft: invalid configuration change")
