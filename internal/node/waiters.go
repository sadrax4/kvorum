package node

import (
	"context"

	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/raft"
)

type proposalKind uint8

const (
	proposalCommand proposalKind = iota + 1
	proposalConfChange
	proposalAttrs
)

type proposalResult struct {
	res kv.Result
	err error
}

// proposal is a request that becomes a log entry. The run loop assigns the
// index and term when raft accepts it and resolves it once the entry at that
// index is applied.
type proposal struct {
	kind  proposalKind
	data  []byte
	cc    raft.ConfChange
	ctx   context.Context
	index raft.Index
	term  raft.Term
	resC  chan proposalResult
}

func newProposal(ctx context.Context, kind proposalKind) *proposal {
	return &proposal{kind: kind, ctx: ctx, resC: make(chan proposalResult, 1)}
}

func (p *proposal) resolve(res kv.Result, err error) {
	select {
	case p.resC <- proposalResult{res: res, err: err}:
	default:
	}
}

// readWaiter is a linearizable read barrier in flight.
type readWaiter struct {
	ctx   context.Context
	id    uint64
	index raft.Index // set once the ReadState arrives
	resC  chan error
}

func (w *readWaiter) resolve(err error) {
	select {
	case w.resC <- err:
	default:
	}
}

// report carries transport feedback into the run loop.
type report struct {
	id       raft.NodeID
	snapshot bool
	ok       bool
}

type transferReq struct {
	to   raft.NodeID
	resC chan error
}

type snapshotReq struct {
	resC chan snapshotResult
}

type snapshotResult struct {
	index raft.Index
	err   error
}
