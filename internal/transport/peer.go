package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

const (
	// clientKeepaliveTime is the idle period after which a client pings the
	// peer. It must not be below the server's enforcement minimum.
	clientKeepaliveTime = 10 * time.Second
	// clientKeepaliveTimeout is how long a client waits for a ping reply
	// before closing the connection.
	clientKeepaliveTimeout = 3 * time.Second
	// snapshotMinThroughput is the rate, in bytes per second, below which a
	// snapshot stream is abandoned. It sizes the stream deadline together
	// with SendTimeout.
	snapshotMinThroughput = 1 << 20
)

// peer is the outbound side of one cluster member: a bounded queue drained by
// a single sender goroutine over a lazily dialled connection.
type peer struct {
	id   raft.NodeID
	addr string
	t    *Transport

	ctx    context.Context
	cancel context.CancelFunc
	queue  chan raft.Message
	logger *slog.Logger

	// unreachable throttles ReportUnreachable and the accompanying log line.
	unreachable *limiter
	// snapshotInFlight is set while a snapshot stream to this peer is open;
	// Raft sends one snapshot at a time, so a second one is dropped.
	snapshotInFlight atomic.Bool

	mu     sync.Mutex
	closed bool
	conn   *grpc.ClientConn
	client raftpb.RaftServiceClient
}

// newPeer creates the peer and starts its sender goroutine. The caller holds
// t.mu and has checked that the transport is not stopped, which is what makes
// the wg.Add safe against Stop's Wait.
func (t *Transport) newPeer(id raft.NodeID, addr string) *peer {
	ctx, cancel := context.WithCancel(t.ctx)
	p := &peer{
		id:          id,
		addr:        addr,
		t:           t,
		ctx:         ctx,
		cancel:      cancel,
		queue:       make(chan raft.Message, t.cfg.QueueSize),
		logger:      t.logger.With(slog.Uint64("peer", uint64(id)), slog.String("addr", addr)),
		unreachable: newLimiter(t.now),
	}
	t.wg.Add(1)
	go p.run()
	return p
}

// run drains the queue until the peer is closed. Messages still queued at
// that point are discarded; Raft retransmits whatever matters.
func (p *peer) run() {
	defer p.t.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case m := <-p.queue:
			p.step(m)
		}
	}
}

// enqueue offers m to the queue without blocking and reports whether it was
// accepted.
func (p *peer) enqueue(m raft.Message) bool {
	select {
	case p.queue <- m:
		return true
	default:
		return false
	}
}

// close stops the sender, aborts in-flight calls, and releases the
// connection. It does not wait for goroutines; the transport's WaitGroup
// does.
func (p *peer) close() {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			p.logger.Debug("closing peer connection", slog.Any("error", err))
		}
		p.conn = nil
		p.client = nil
	}
}

// getClient returns the RPC client, dialling on first use. grpc.NewClient
// does not connect; the first call does, bounded by DialTimeout per attempt.
func (p *peer) getClient() (raftpb.RaftServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrStopped
	}
	if p.client != nil {
		return p.client, nil
	}
	conn, err := grpc.NewClient(p.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                clientKeepaliveTime,
			Timeout:             clientKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: p.t.cfg.DialTimeout,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.t.cfg.MaxMessageSize),
			grpc.MaxCallSendMsgSize(p.t.cfg.MaxMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	p.conn = conn
	p.client = raftpb.NewRaftServiceClient(conn)
	return p.client, nil
}

// step delivers one queued message with a unary call.
func (p *peer) step(m raft.Message) {
	if p.ctx.Err() != nil {
		return
	}
	client, err := p.getClient()
	if err != nil {
		p.fail(m.Type, err)
		return
	}
	req := &raftpb.StepRequest{Message: ToProto(m)}
	ctx, cancel := context.WithTimeout(p.ctx, p.t.cfg.SendTimeout)
	defer cancel()
	if _, err := client.Step(ctx, req); err != nil {
		p.fail(m.Type, err)
		return
	}
	p.t.metrics.MessageSent(m.Type, proto.Size(req))
}

// fail records a delivery failure and reports the peer unreachable, at most
// once per rateLimitInterval. Failures caused by the transport stopping are
// ignored.
func (p *peer) fail(mt raft.MessageType, err error) {
	if p.ctx.Err() != nil {
		return
	}
	p.t.metrics.SendFailed(p.id)
	if !p.unreachable.allow(p.id) {
		return
	}
	p.logger.Warn("peer unreachable", slog.String("type", mt.String()), slog.Any("error", err))
	p.t.handler.ReportUnreachable(p.id)
}

// sendSnapshot starts a goroutine that streams m's snapshot to the peer. The
// caller holds t.mu (read) and has checked that the transport is not stopped.
// A MsgSnap without a payload, or one that arrives while another snapshot is
// in flight, is dropped; in the first case Raft is told the transfer failed
// so it can retry, in the second the in-flight transfer reports for both.
func (p *peer) sendSnapshot(m raft.Message) {
	if m.Snapshot == nil {
		p.logger.Warn("dropping MsgSnap without snapshot")
		p.t.metrics.Dropped(p.id)
		p.t.handler.ReportSnapshot(p.id, false)
		return
	}
	if !p.snapshotInFlight.CompareAndSwap(false, true) {
		p.logger.Debug("dropping MsgSnap: snapshot already in flight")
		p.t.metrics.Dropped(p.id)
		return
	}
	p.t.wg.Add(1)
	go func() {
		defer p.t.wg.Done()
		defer p.snapshotInFlight.Store(false)
		ok := p.streamSnapshot(m)
		if p.ctx.Err() != nil && !ok {
			return
		}
		p.t.handler.ReportSnapshot(p.id, ok)
		p.t.metrics.SnapshotSent(p.id, ok)
	}()
}

// streamSnapshot performs one InstallSnapshot call and reports success.
func (p *peer) streamSnapshot(m raft.Message) bool {
	snap := m.Snapshot
	logger := p.logger.With(
		slog.Uint64("index", uint64(snap.Meta.Index)),
		slog.Uint64("term", uint64(snap.Meta.Term)),
		slog.Int("bytes", len(snap.Data)),
	)
	client, err := p.getClient()
	if err != nil {
		p.fail(raft.MsgSnap, err)
		return false
	}
	timeout := p.t.cfg.SendTimeout + time.Duration(len(snap.Data)/snapshotMinThroughput)*time.Second
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		p.fail(raft.MsgSnap, err)
		return false
	}
	header := &raftpb.InstallSnapshotRequest{
		Payload: &raftpb.InstallSnapshotRequest_Header{Header: ToProto(m)},
	}
	sent := proto.Size(header)
	if err := p.sendChunks(stream, header, snap.Data); err != nil {
		// Send reports io.EOF when the server has already failed the
		// stream; the status comes back from CloseAndRecv.
		if _, rerr := stream.CloseAndRecv(); rerr != nil {
			err = rerr
		}
		logger.Warn("snapshot stream failed", slog.Any("error", err))
		p.fail(raft.MsgSnap, err)
		return false
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Warn("snapshot rejected", slog.Any("error", err))
		p.fail(raft.MsgSnap, err)
		return false
	}
	p.t.metrics.MessageSent(raft.MsgSnap, sent+len(snap.Data))
	logger.Info("snapshot sent")
	return true
}

// sendChunks writes the header followed by data in SnapshotChunkSize pieces.
func (p *peer) sendChunks(stream raftpb.RaftService_InstallSnapshotClient, header *raftpb.InstallSnapshotRequest, data []byte) error {
	if err := stream.Send(header); err != nil {
		return fmt.Errorf("send header: %w", err)
	}
	chunk := p.t.cfg.SnapshotChunkSize
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		req := &raftpb.InstallSnapshotRequest{
			Payload: &raftpb.InstallSnapshotRequest_Data{Data: data[off:end]},
		}
		if err := stream.Send(req); err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("send chunk at %d: stream closed by server: %w", off, err)
			}
			return fmt.Errorf("send chunk at %d: %w", off, err)
		}
	}
	return nil
}
