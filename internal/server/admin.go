package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/raft"
)

type adminService struct {
	kvorumpb.UnimplementedAdminServiceServer
	s *Server
}

func (a *adminService) header() *kvorumpb.ResponseHeader {
	return &kvorumpb.ResponseHeader{
		MemberId: uint64(a.s.node.ID()),
		Term:     uint64(a.s.node.Term()),
		Revision: uint64(a.s.node.AppliedIndex()),
	}
}

func toProtoMembers(members []node.Member) []*kvorumpb.Member {
	out := make([]*kvorumpb.Member, 0, len(members))
	for _, m := range members {
		out = append(out, &kvorumpb.Member{
			Id:            uint64(m.ID),
			RaftAddress:   m.RaftAddress,
			ClientAddress: m.ClientAddress,
			Learner:       m.Learner,
		})
	}
	return out
}

// Status implements kvorumpb.AdminServiceServer.
func (a *adminService) Status(context.Context, *kvorumpb.StatusRequest) (*kvorumpb.StatusResponse, error) {
	st := a.s.node.Status()
	return &kvorumpb.StatusResponse{
		Header:        a.header(),
		Id:            uint64(st.ID),
		State:         st.State.String(),
		Term:          uint64(st.Term),
		Leader:        uint64(st.Leader),
		CommitIndex:   uint64(st.Commit),
		AppliedIndex:  uint64(st.Applied),
		LastIndex:     uint64(st.LastIndex),
		SnapshotIndex: uint64(st.SnapshotIndex),
		Members:       toProtoMembers(st.Members),
		Version:       telemetry.Version,
	}, nil
}

// ListMembers implements kvorumpb.AdminServiceServer.
func (a *adminService) ListMembers(context.Context, *kvorumpb.ListMembersRequest) (*kvorumpb.ListMembersResponse, error) {
	resp := &kvorumpb.ListMembersResponse{Header: a.header(), Members: toProtoMembers(a.s.node.Members())}
	if leader, ok := a.s.node.Leader(); ok {
		resp.Leader = uint64(leader.ID)
	}
	return resp, nil
}

// AddMember implements kvorumpb.AdminServiceServer.
func (a *adminService) AddMember(ctx context.Context, req *kvorumpb.AddMemberRequest) (*kvorumpb.AddMemberResponse, error) {
	m := req.GetMember()
	if m == nil || m.GetId() == 0 || m.GetRaftAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "member id and raft address are required")
	}
	err := a.s.node.AddMember(ctx, node.Member{
		ID:            raft.NodeID(m.GetId()),
		RaftAddress:   m.GetRaftAddress(),
		ClientAddress: m.GetClientAddress(),
		Learner:       m.GetLearner(),
	})
	if err != nil {
		return nil, a.s.toStatus(err)
	}
	return &kvorumpb.AddMemberResponse{Header: a.header(), Members: toProtoMembers(a.s.node.Members())}, nil
}

// RemoveMember implements kvorumpb.AdminServiceServer.
func (a *adminService) RemoveMember(ctx context.Context, req *kvorumpb.RemoveMemberRequest) (*kvorumpb.RemoveMemberResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "member id is required")
	}
	if err := a.s.node.RemoveMember(ctx, raft.NodeID(req.GetId())); err != nil {
		return nil, a.s.toStatus(err)
	}
	return &kvorumpb.RemoveMemberResponse{Header: a.header(), Members: toProtoMembers(a.s.node.Members())}, nil
}

// PromoteMember implements kvorumpb.AdminServiceServer.
func (a *adminService) PromoteMember(ctx context.Context, req *kvorumpb.PromoteMemberRequest) (*kvorumpb.PromoteMemberResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "member id is required")
	}
	if err := a.s.node.PromoteMember(ctx, raft.NodeID(req.GetId())); err != nil {
		return nil, a.s.toStatus(err)
	}
	return &kvorumpb.PromoteMemberResponse{Header: a.header(), Members: toProtoMembers(a.s.node.Members())}, nil
}

// TransferLeadership implements kvorumpb.AdminServiceServer.
func (a *adminService) TransferLeadership(ctx context.Context, req *kvorumpb.TransferLeadershipRequest) (*kvorumpb.TransferLeadershipResponse, error) {
	if req.GetTargetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "target id is required")
	}
	if err := a.s.node.TransferLeadership(ctx, raft.NodeID(req.GetTargetId())); err != nil {
		return nil, a.s.toStatus(err)
	}
	return &kvorumpb.TransferLeadershipResponse{Header: a.header()}, nil
}

// Snapshot implements kvorumpb.AdminServiceServer.
func (a *adminService) Snapshot(ctx context.Context, _ *kvorumpb.SnapshotRequest) (*kvorumpb.SnapshotResponse, error) {
	idx, err := a.s.node.Snapshot(ctx)
	if err != nil {
		return nil, a.s.toStatus(err)
	}
	return &kvorumpb.SnapshotResponse{Header: a.header(), SnapshotIndex: uint64(idx)}, nil
}
