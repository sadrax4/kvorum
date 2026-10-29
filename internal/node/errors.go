package node

import "errors"

var (
	// ErrNotLeader is returned for operations that must run on the leader.
	// Callers can use Node.Leader to redirect.
	ErrNotLeader = errors.New("node: not the leader")

	// ErrNoLeader is returned when the node does not know a leader, for
	// example during an election or while partitioned from the quorum.
	ErrNoLeader = errors.New("node: no leader elected")

	// ErrLeadershipTransferring is returned while the leader is handing over
	// to another node and therefore refuses new proposals. It clears within
	// an election timeout; clients should back off and retry.
	ErrLeadershipTransferring = errors.New("node: leadership transfer in progress")

	// ErrProposalLost is returned when the node lost leadership after
	// accepting a proposal and a different entry was committed in its place.
	// The proposal may or may not have been applied by the new leader; with a
	// client session a retry is safe.
	ErrProposalLost = errors.New("node: proposal lost after leadership change")

	// ErrStopped is returned once the node has been stopped.
	ErrStopped = errors.New("node: stopped")

	// ErrRemoved is returned once the node has been removed from the cluster.
	ErrRemoved = errors.New("node: removed from the cluster")

	// ErrQueueFull is returned when the request queue into the run loop is
	// saturated.
	ErrQueueFull = errors.New("node: request queue is full")

	// ErrInvalidMember is returned for membership requests with missing or
	// malformed member information.
	ErrInvalidMember = errors.New("node: invalid member")
)
