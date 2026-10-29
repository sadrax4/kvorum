package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/raft"
)

// toStatus converts an internal error into a gRPC status. Leadership errors
// carry a LeaderHint detail so that clients can follow the leader.
func (s *Server) toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, node.ErrNotLeader):
		return s.notLeader("not the leader")
	case errors.Is(err, node.ErrNoLeader):
		return s.notLeader("no leader elected")
	case errors.Is(err, node.ErrProposalLost):
		return s.notLeader("leadership changed before the request was applied; retry")
	case errors.Is(err, raft.ErrConfChangePending), errors.Is(err, raft.ErrLeaderTermNotCommitted):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, node.ErrStopped), errors.Is(err, node.ErrRemoved):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, node.ErrQueueFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, kv.ErrInvalidCommand), errors.Is(err, node.ErrInvalidMember):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrStaleSequence):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrInvalidConfChange):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrUnknownNode):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, kv.ErrWatcherOverflow):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		s.logger.Error("internal error", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// notLeader builds an Unavailable status with a LeaderHint detail.
func (s *Server) notLeader(msg string) error {
	st := status.New(codes.Unavailable, msg)
	hint := &kvorumpb.LeaderHint{}
	if leader, ok := s.node.Leader(); ok {
		hint.LeaderId = uint64(leader.ID)
		hint.LeaderAddress = leader.ClientAddress
	}
	if withDetails, err := st.WithDetails(hint); err == nil {
		st = withDetails
	}
	return st.Err()
}
