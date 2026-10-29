package node

import (
	"context"
	"fmt"

	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/raft"
)

// Propose replicates a command through Raft and returns its result once it
// has been applied on this node. It returns ErrNotLeader on followers.
func (n *Node) Propose(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	if err := cmd.Validate(); err != nil {
		return kv.Result{}, err
	}
	p := newProposal(ctx, proposalCommand)
	p.data = encodeEntry(entryKindCommand, cmd.Marshal())
	return n.await(p)
}

func (n *Node) await(p *proposal) (kv.Result, error) {
	select {
	case n.proposeC <- p:
	case <-p.ctx.Done():
		return kv.Result{}, p.ctx.Err()
	case <-n.stopC:
		return kv.Result{}, ErrStopped
	}
	select {
	case r := <-p.resC:
		return r.res, r.err
	case <-p.ctx.Done():
		return kv.Result{}, p.ctx.Err()
	case <-n.stopC:
		return kv.Result{}, ErrStopped
	}
}

// LinearizableRead blocks until the local state machine reflects every write
// that was committed before the call started. It works on followers too: the
// read index is obtained from the leader and the follower waits until it has
// applied up to it.
func (n *Node) LinearizableRead(ctx context.Context) error {
	r := &readWaiter{ctx: ctx, resC: make(chan error, 1)}
	select {
	case n.readC <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
	select {
	case err := <-r.resC:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
}

// AddMember adds a voter or learner to the cluster.
func (n *Node) AddMember(ctx context.Context, m Member) error {
	if m.ID == raft.None || m.RaftAddress == "" {
		return fmt.Errorf("%w: id and raft address are required", ErrInvalidMember)
	}
	typ := raft.ConfChangeAddVoter
	if m.Learner {
		typ = raft.ConfChangeAddLearner
	}
	return n.proposeConfChange(ctx, raft.ConfChange{
		Type:    typ,
		NodeID:  m.ID,
		Context: encodeMemberContext(memberContext{RaftAddress: m.RaftAddress, ClientAddress: m.ClientAddress}),
	})
}

// PromoteMember turns a learner into a voter.
func (n *Node) PromoteMember(ctx context.Context, id raft.NodeID) error {
	n.mu.RLock()
	isLearner := n.confState.IsLearner(id)
	n.mu.RUnlock()
	if !isLearner {
		return fmt.Errorf("%w: node %d is not a learner", ErrInvalidMember, id)
	}
	return n.proposeConfChange(ctx, raft.ConfChange{Type: raft.ConfChangeAddVoter, NodeID: id})
}

// RemoveMember removes a member from the cluster. Removing the local node is
// allowed; the node steps down and Removed is closed once the change applies.
func (n *Node) RemoveMember(ctx context.Context, id raft.NodeID) error {
	if id == raft.None {
		return fmt.Errorf("%w: id is required", ErrInvalidMember)
	}
	return n.proposeConfChange(ctx, raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: id})
}

func (n *Node) proposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	p := newProposal(ctx, proposalConfChange)
	p.cc = cc
	_, err := n.await(p)
	return err
}

// TransferLeadership asks the leader to hand over to another voter. It
// returns once raft has accepted the request; the transfer itself completes
// asynchronously within an election timeout or is abandoned.
func (n *Node) TransferLeadership(ctx context.Context, to raft.NodeID) error {
	req := &transferReq{to: to, resC: make(chan error, 1)}
	select {
	case n.transferC <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
	select {
	case err := <-req.resC:
		if err != nil {
			return translateProposeError(err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
}

// Snapshot forces a snapshot and log compaction and returns the snapshot
// index.
func (n *Node) Snapshot(ctx context.Context) (raft.Index, error) {
	req := &snapshotReq{resC: make(chan snapshotResult, 1)}
	select {
	case n.snapshotC <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopC:
		return 0, ErrStopped
	}
	select {
	case r := <-req.resC:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopC:
		return 0, ErrStopped
	}
}
