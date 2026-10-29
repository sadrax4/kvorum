package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

const (
	// stopGracePeriod is how long Stop lets in-flight server calls finish
	// before closing their connections.
	stopGracePeriod = time.Second
)

// Publisher is implemented by the node to record attributes that a member
// publishes through the transport, such as its client address.
type Publisher interface {
	PublishAttributes(ctx context.Context, id raft.NodeID, clientAddress string) error
}

// Transport exchanges Raft messages with the other members of the cluster.
// Create one with [New]; all methods are safe for concurrent use.
type Transport struct {
	cfg     Config
	logger  *slog.Logger
	metrics Metrics
	handler Handler
	// now is the clock used by rate limiters; tests substitute it.
	now func() time.Time

	// ctx is cancelled by Stop and bounds every outbound call, every
	// handler invocation, and every goroutine.
	ctx    context.Context
	cancel context.CancelFunc
	// wg tracks the serve goroutine, peer senders, and snapshot streams.
	// Every Add happens under mu while stopped is false, so Stop's Wait
	// cannot race with an Add.
	wg sync.WaitGroup

	// unknownLog and dropLog throttle diagnostics for dropped messages.
	unknownLog *limiter
	dropLog    *limiter

	mu       sync.RWMutex
	peers    map[raft.NodeID]*peer
	listener net.Listener
	server   *grpc.Server
	started  bool
	stopped  bool
}

// New validates cfg and returns a transport that is not yet serving. Peers
// may be added before [Transport.Start].
func New(cfg Config) (*Transport, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		cfg:     cfg,
		logger:  cfg.Logger.With(slog.String("component", "transport"), slog.Uint64("local", uint64(cfg.LocalID))),
		metrics: cfg.Metrics,
		handler: cfg.Handler,
		now:     time.Now,
		ctx:     ctx,
		cancel:  cancel,
		peers:   make(map[raft.NodeID]*peer),
	}
	t.unknownLog = newLimiter(t.now)
	t.dropLog = newLimiter(t.now)
	return t, nil
}

// Start binds the listener and serves RaftService in a background goroutine.
// It returns [ErrAlreadyStarted] on a second call and [ErrStopped] after
// Stop.
func (t *Transport) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return ErrStopped
	}
	if t.started {
		return ErrAlreadyStarted
	}
	lis, err := net.Listen("tcp", t.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("transport: listen on %s: %w", t.cfg.ListenAddress, err)
	}
	srv := newGRPCServer(t.cfg)
	raftpb.RegisterRaftServiceServer(srv, &raftServer{t: t})
	t.listener = lis
	t.server = srv
	t.started = true
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.logger.Error("serve", slog.Any("error", err))
		}
	}()
	t.logger.Info("listening", slog.String("addr", lis.Addr().String()))
	return nil
}

// Stop cancels in-flight calls, closes every peer, stops the server, and
// waits for all goroutines the transport started. It is idempotent and
// returns promptly even when queues are full.
func (t *Transport) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.cancel()
	peers := t.peers
	t.peers = make(map[raft.NodeID]*peer)
	srv := t.server
	t.mu.Unlock()

	for _, p := range peers {
		p.close()
	}
	if srv != nil {
		stopServer(srv)
	}
	t.wg.Wait()
	t.logger.Info("stopped")
}

// stopServer tries a graceful stop and falls back to a hard stop once the
// grace period elapses, so a stalled client cannot hold Stop hostage.
func stopServer(srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.GracefulStop()
	}()
	timer := time.NewTimer(stopGracePeriod)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		srv.Stop()
		<-done
	}
}

// Address returns the bound listener address, which differs from
// [Config.ListenAddress] when port 0 was requested. It is empty before Start.
func (t *Transport) Address() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.listener == nil {
		return ""
	}
	return t.listener.Addr().String()
}

// Send queues msgs for delivery and returns immediately. Messages to unknown
// peers, messages that do not fit in a peer's queue, and all messages after
// Stop are dropped. A MsgSnap is streamed on its own goroutine.
func (t *Transport) Send(msgs []raft.Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.stopped {
		return
	}
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			t.metrics.Dropped(m.To)
			if t.unknownLog.allow(m.To) {
				t.logger.Debug("dropping message to unknown peer",
					slog.Uint64("to", uint64(m.To)), slog.String("type", m.Type.String()))
			}
			continue
		}
		if m.Type == raft.MsgSnap {
			p.sendSnapshot(m)
			continue
		}
		if !p.enqueue(m) {
			t.metrics.Dropped(m.To)
			if t.dropLog.allow(m.To) {
				t.logger.Debug("dropping message: peer queue full",
					slog.Uint64("to", uint64(m.To)), slog.String("type", m.Type.String()))
			}
		}
	}
}

// AddPeer registers addr as the address of id. Adding an existing peer with
// the same address is a no-op; a different address closes the old connection
// and dials the new one on the next send. The local id, the zero id, and an
// empty address are rejected with a log line.
func (t *Transport) AddPeer(id raft.NodeID, addr string) {
	if id == raft.None || id == t.cfg.LocalID || addr == "" {
		t.logger.Warn("ignoring invalid peer", slog.Uint64("id", uint64(id)), slog.String("addr", addr))
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	if old, ok := t.peers[id]; ok {
		if old.addr == addr {
			return
		}
		old.close()
		t.logger.Info("peer address changed", slog.Uint64("peer", uint64(id)),
			slog.String("old", old.addr), slog.String("new", addr))
	} else {
		t.logger.Info("peer added", slog.Uint64("peer", uint64(id)), slog.String("addr", addr))
	}
	t.peers[id] = t.newPeer(id, addr)
}

// RemovePeer forgets id, closing its connection and discarding its queue.
// Removing an unknown peer is a no-op.
func (t *Transport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, ok := t.peers[id]
	if !ok {
		return
	}
	delete(t.peers, id)
	p.close()
	t.unknownLog.forget(id)
	t.dropLog.forget(id)
	t.logger.Info("peer removed", slog.Uint64("peer", uint64(id)))
}

// Peers returns a copy of the known peer addresses keyed by node id.
func (t *Transport) Peers() map[raft.NodeID]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[raft.NodeID]string, len(t.peers))
	for id, p := range t.peers {
		out[id] = p.addr
	}
	return out
}

// requestContext derives from ctx a context that is also cancelled when the
// transport stops, so handlers blocked on a request return during Stop.
func (t *Transport) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(t.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// Publish sends this node's client address to the peer with the given id,
// which is expected to be the leader. It blocks until the peer answers or
// ctx expires. It returns ErrStopped after Stop and an error if the peer is
// unknown.
func (t *Transport) Publish(ctx context.Context, to raft.NodeID, clientAddress string) error {
	t.mu.RLock()
	p, ok := t.peers[to]
	stopped := t.stopped
	t.mu.RUnlock()
	if stopped {
		return ErrStopped
	}
	if !ok {
		return fmt.Errorf("transport: publish: unknown peer %d", to)
	}
	client, err := p.getClient()
	if err != nil {
		return fmt.Errorf("transport: publish to %d: %w", to, err)
	}
	ctx, cancel := t.requestContext(ctx)
	defer cancel()
	if _, err := client.Publish(ctx, &raftpb.PublishRequest{MemberId: uint64(t.cfg.LocalID), ClientAddress: clientAddress}); err != nil {
		return fmt.Errorf("transport: publish to %d: %w", to, err)
	}
	return nil
}
