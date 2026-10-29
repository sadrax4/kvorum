package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/sadrax4/kvorum/api/kvorumpb"
)

// Status reports the Raft state of the node currently preferred by the
// client. Call it after Members or a write to target the leader.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	var resp *kvorumpb.StatusResponse
	err := c.invoke(ctx, "status", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).Status(ctx, &kvorumpb.StatusRequest{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Status{
		Header:        headerFromProto(resp.GetHeader()),
		ID:            resp.GetId(),
		State:         resp.GetState(),
		Term:          resp.GetTerm(),
		Leader:        resp.GetLeader(),
		CommitIndex:   resp.GetCommitIndex(),
		AppliedIndex:  resp.GetAppliedIndex(),
		LastIndex:     resp.GetLastIndex(),
		SnapshotIndex: resp.GetSnapshotIndex(),
		Members:       membersFromProto(resp.GetMembers()),
		Version:       resp.GetVersion(),
	}, nil
}

// Members returns the current cluster membership and the id of the leader
// known to the serving node (zero when unknown).
func (c *Client) Members(ctx context.Context) ([]Member, uint64, error) {
	var resp *kvorumpb.ListMembersResponse
	err := c.invoke(ctx, "members", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).ListMembers(ctx, &kvorumpb.ListMembersRequest{})
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return membersFromProto(resp.GetMembers()), resp.GetLeader(), nil
}

// AddMember adds a voter or learner and returns the resulting membership.
// Adding as a learner first and promoting once it has caught up avoids
// availability dips.
func (c *Client) AddMember(ctx context.Context, m Member) ([]Member, error) {
	req := &kvorumpb.AddMemberRequest{Member: memberToProto(m)}
	var resp *kvorumpb.AddMemberResponse
	err := c.invoke(ctx, "add-member", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).AddMember(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return membersFromProto(resp.GetMembers()), nil
}

// RemoveMember removes a voter or learner and returns the resulting
// membership.
func (c *Client) RemoveMember(ctx context.Context, id uint64) ([]Member, error) {
	req := &kvorumpb.RemoveMemberRequest{Id: id}
	var resp *kvorumpb.RemoveMemberResponse
	err := c.invoke(ctx, "remove-member", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).RemoveMember(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return membersFromProto(resp.GetMembers()), nil
}

// PromoteMember turns a learner into a voter and returns the resulting
// membership.
func (c *Client) PromoteMember(ctx context.Context, id uint64) ([]Member, error) {
	req := &kvorumpb.PromoteMemberRequest{Id: id}
	var resp *kvorumpb.PromoteMemberResponse
	err := c.invoke(ctx, "promote-member", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).PromoteMember(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return membersFromProto(resp.GetMembers()), nil
}

// TransferLeadership asks the leader to hand leadership to targetID.
func (c *Client) TransferLeadership(ctx context.Context, targetID uint64) error {
	req := &kvorumpb.TransferLeadershipRequest{TargetId: targetID}
	return c.invoke(ctx, "transfer-leadership", func(ctx context.Context, cc *grpc.ClientConn) error {
		_, err := kvorumpb.NewAdminServiceClient(cc).TransferLeadership(ctx, req)
		return err
	})
}

// Snapshot forces the serving node to take a snapshot and compact its log. It
// returns the index of the new snapshot.
func (c *Client) Snapshot(ctx context.Context) (uint64, error) {
	var resp *kvorumpb.SnapshotResponse
	err := c.invoke(ctx, "snapshot", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewAdminServiceClient(cc).Snapshot(ctx, &kvorumpb.SnapshotRequest{})
		return err
	})
	if err != nil {
		return 0, err
	}
	return resp.GetSnapshotIndex(), nil
}
