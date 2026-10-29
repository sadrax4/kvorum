package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/internal/transport"
	"github.com/sadrax4/kvorum/internal/wal"
	"github.com/sadrax4/kvorum/raft"
)

// Config configures a Node.
type Config struct {
	// ID is the node's identity in the cluster. Required.
	ID raft.NodeID
	// DataDir holds the write-ahead log and snapshots. Required.
	DataDir string

	// ListenRaft is the address the Raft transport binds to.
	ListenRaft string
	// AdvertiseRaft is the address peers use to reach this node. Defaults to
	// ListenRaft.
	AdvertiseRaft string
	// AdvertiseClient is the client-facing address this node publishes so
	// that other nodes can redirect clients to it when it is the leader.
	AdvertiseClient string

	// InitialCluster maps member ids to their Raft addresses. For a new
	// cluster it is the bootstrap membership; with Join it lists the existing
	// members to contact.
	InitialCluster map[raft.NodeID]string
	// Join starts the node without bootstrapping; it waits for an existing
	// leader to add it.
	Join bool

	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int

	// SnapshotThreshold is the number of applied entries after which a
	// snapshot is taken and the log compacted.
	SnapshotThreshold uint64
	// SnapshotCatchUpEntries is the number of entries retained after
	// compaction so that slightly lagging followers can catch up without a
	// snapshot.
	SnapshotCatchUpEntries uint64

	MaxSizePerMsg   uint64
	MaxInflightMsgs int
	WALSegmentSize  int64

	// RequestQueueSize bounds the number of client requests waiting to enter
	// the run loop.
	RequestQueueSize int

	Logger  *slog.Logger
	Metrics *telemetry.Metrics
}

func (c *Config) withDefaults() (Config, error) {
	cfg := *c
	if cfg.ID == raft.None {
		return cfg, errors.New("node: ID is required")
	}
	if cfg.DataDir == "" {
		return cfg, errors.New("node: DataDir is required")
	}
	if cfg.ListenRaft == "" {
		return cfg, errors.New("node: ListenRaft is required")
	}
	if cfg.AdvertiseRaft == "" {
		cfg.AdvertiseRaft = cfg.ListenRaft
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 10000
	}
	if cfg.SnapshotCatchUpEntries == 0 {
		cfg.SnapshotCatchUpEntries = 1000
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = 1 << 20
	}
	if cfg.MaxInflightMsgs == 0 {
		cfg.MaxInflightMsgs = 256
	}
	if cfg.RequestQueueSize <= 0 {
		cfg.RequestQueueSize = 4096
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Metrics == nil {
		return cfg, errors.New("node: Metrics is required")
	}
	if !cfg.Join && len(cfg.InitialCluster) == 0 {
		return cfg, errors.New("node: InitialCluster is required unless Join is set")
	}
	if cfg.Join && len(cfg.InitialCluster) == 0 {
		return cfg, errors.New("node: Join requires InitialCluster to list the existing members")
	}
	return cfg, nil
}

// Node is a member of a kvorum cluster.
type Node struct {
	cfg     Config
	logger  *slog.Logger
	metrics *telemetry.Metrics

	rn          *raft.RawNode
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	snapshotter *wal.Snapshotter
	store       *kv.Store
	transport   *transport.Transport

	// mu guards the fields below, which are read by other goroutines.
	mu        sync.RWMutex
	members   snapshotMembers
	confState raft.ConfState
	leader    raft.NodeID
	term      raft.Term
	state     raft.StateType
	lastIndex raft.Index
	progress  map[raft.NodeID]raft.ProgressStatus

	appliedIndex  atomic.Uint64
	commitIndex   atomic.Uint64
	snapshotIndex atomic.Uint64

	// Channels into the run loop.
	proposeC  chan *proposal
	readC     chan *readWaiter
	msgC      chan raft.Message
	reportC   chan report
	transferC chan *transferReq
	snapshotC chan *snapshotReq
	publishC  chan publishResult

	// State owned by the run loop.
	waiters      map[raft.Index]*proposal
	readWaiters  map[uint64]*readWaiter
	pendingReads []*readWaiter
	readSeq      uint64
	ticks        uint64
	// publishedAt is the term in which this node last published its client
	// address (by proposing it as leader, or through the leader as a
	// follower); publishInFlight guards the follower's asynchronous RPC.
	publishedAt     raft.Term
	publishInFlight bool

	removedOnce sync.Once
	removedC    chan struct{}
	stopOnce    sync.Once
	stopC       chan struct{}
	doneC       chan struct{}
	started     atomic.Bool
}

// Open restores the node's state from DataDir (or initialises a new node)
// without starting any goroutine or listener.
func Open(c Config) (*Node, error) {
	cfg, err := c.withDefaults()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		logger:      cfg.Logger.With("node_id", cfg.ID),
		metrics:     cfg.Metrics,
		store:       kv.NewStore(kv.Options{}),
		members:     make(snapshotMembers),
		proposeC:    make(chan *proposal, cfg.RequestQueueSize),
		readC:       make(chan *readWaiter, cfg.RequestQueueSize),
		msgC:        make(chan raft.Message, cfg.RequestQueueSize),
		reportC:     make(chan report, 1024),
		transferC:   make(chan *transferReq),
		snapshotC:   make(chan *snapshotReq),
		publishC:    make(chan publishResult, 1),
		waiters:     make(map[raft.Index]*proposal),
		readWaiters: make(map[uint64]*readWaiter),
		removedC:    make(chan struct{}),
		stopC:       make(chan struct{}),
		doneC:       make(chan struct{}),
	}

	fresh, applied, err := n.openStorage(filepath.Join(cfg.DataDir, "wal"), filepath.Join(cfg.DataDir, "snap"))
	if err != nil {
		return nil, err
	}

	rn, err := raft.NewRawNode(&raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         n.raftStorage,
		Applied:         applied,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		PreVote:         true,
		CheckQuorum:     true,
		Logger:          n.logger.With("component", "raft"),
	})
	if err != nil {
		_ = n.wal.Close()
		return nil, fmt.Errorf("node: create raft node: %w", err)
	}
	n.rn = rn

	if fresh {
		for id, addr := range cfg.InitialCluster {
			n.members[id] = memberContext{RaftAddress: addr}
		}
		if cfg.Join {
			n.logger.Info("starting as a joining node; waiting for the leader", "peers", cfg.InitialCluster)
		} else {
			if err := rn.Bootstrap(bootstrapPeers(cfg.InitialCluster)); err != nil {
				_ = n.wal.Close()
				return nil, fmt.Errorf("node: bootstrap: %w", err)
			}
			n.logger.Info("bootstrapped new cluster", "members", cfg.InitialCluster)
		}
	}
	n.members[cfg.ID] = memberContext{RaftAddress: cfg.AdvertiseRaft, ClientAddress: cfg.AdvertiseClient}
	n.confState = rn.ConfState()
	// Read contexts must not repeat across restarts: a response to a read
	// issued by a previous incarnation of this process must never satisfy a
	// newer read. Starting from the clock makes that practically impossible.
	n.readSeq = uint64(time.Now().UnixNano())
	n.refreshStatusLocked(rn.BasicStatus())

	t, err := transport.New(transport.Config{
		LocalID:       cfg.ID,
		ListenAddress: cfg.ListenRaft,
		Handler:       (*transportHandler)(n),
		Publisher:     (*attributePublisher)(n),
		Logger:        n.logger.With("component", "transport"),
		Metrics:       &transportMetrics{m: n.metrics},
	})
	if err != nil {
		_ = n.wal.Close()
		return nil, fmt.Errorf("node: create transport: %w", err)
	}
	n.transport = t
	for id, mc := range n.members {
		if id != cfg.ID && mc.RaftAddress != "" {
			t.AddPeer(id, mc.RaftAddress)
		}
	}
	return n, nil
}

// Start binds the Raft transport and starts the run loop.
func (n *Node) Start() error {
	if n.started.Swap(true) {
		return errors.New("node: already started")
	}
	if err := n.transport.Start(); err != nil {
		return fmt.Errorf("node: start transport: %w", err)
	}
	n.logger.Info("node started", "raft_address", n.transport.Address(), "advertise_raft", n.cfg.AdvertiseRaft)
	go n.run()
	return nil
}

// Stop shuts the node down. If the node is the leader and the context
// allows, it first hands leadership to the most up-to-date follower so that
// the cluster does not wait for an election timeout.
func (n *Node) Stop(ctx context.Context) error {
	if !n.started.Load() {
		return n.wal.Close()
	}
	n.stopOnce.Do(func() {
		n.handoverLeadership(ctx)
		close(n.stopC)
	})
	select {
	case <-n.doneC:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handoverLeadership tries to transfer leadership before shutting down.
func (n *Node) handoverLeadership(ctx context.Context) {
	st := n.Status()
	if st.State != raft.StateLeader || len(st.ConfState.Voters) < 2 {
		return
	}
	var best raft.NodeID
	var bestMatch raft.Index
	for id, pr := range st.Progress {
		if id == n.cfg.ID || pr.IsLearner || !st.ConfState.IsVoter(id) {
			continue
		}
		if pr.Match >= bestMatch && pr.RecentActive {
			best, bestMatch = id, pr.Match
		}
	}
	if best == raft.None {
		return
	}
	deadline := time.Duration(n.cfg.ElectionTick) * n.cfg.TickInterval * 2
	tctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	if err := n.TransferLeadership(tctx, best); err != nil {
		n.logger.Warn("leadership handover before shutdown failed", "to", best, "error", err)
		return
	}
	for {
		if n.Status().State != raft.StateLeader {
			n.logger.Info("handed over leadership before shutdown", "to", best)
			return
		}
		select {
		case <-tctx.Done():
			n.logger.Warn("leadership handover timed out", "to", best)
			return
		case <-time.After(n.cfg.TickInterval):
		}
	}
}

// Store returns the key-value state machine for local reads.
func (n *Node) Store() *kv.Store { return n.store }

// Term returns the current Raft term as last observed by the run loop.
func (n *Node) Term() raft.Term {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

// AppliedIndex returns the index of the last applied entry, which doubles as
// the store revision.
func (n *Node) AppliedIndex() raft.Index { return raft.Index(n.appliedIndex.Load()) }

// IsLeader reports whether the node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == raft.StateLeader
}

// ID returns the node id.
func (n *Node) ID() raft.NodeID { return n.cfg.ID }

// RaftAddress returns the bound Raft transport address.
func (n *Node) RaftAddress() string { return n.transport.Address() }

// Removed returns a channel that is closed once the node has been removed
// from the cluster, at which point the process should exit.
func (n *Node) Removed() <-chan struct{} { return n.removedC }

// Status is a point-in-time view of the node for monitoring.
type Status struct {
	raft.Status
	Members       []Member
	SnapshotIndex raft.Index
	ClientAddress string
	RaftAddress   string
}

// Status returns the node's current state. It is served from the run loop
// so that the Raft status is consistent.
func (n *Node) Status() Status {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return Status{
		Status:        n.raftStatusLocked(),
		Members:       n.membersLocked(),
		SnapshotIndex: raft.Index(n.snapshotIndex.Load()),
		ClientAddress: n.cfg.AdvertiseClient,
		RaftAddress:   n.cfg.AdvertiseRaft,
	}
}

// raftStatusLocked builds a raft.Status from the fields mirrored by the run
// loop; the Progress map is only available while the node is leader and is
// refreshed on every Ready.
func (n *Node) raftStatusLocked() raft.Status {
	return raft.Status{
		ID:        n.cfg.ID,
		State:     n.state,
		Term:      n.term,
		Leader:    n.leader,
		Commit:    raft.Index(n.commitIndex.Load()),
		Applied:   raft.Index(n.appliedIndex.Load()),
		LastIndex: n.lastIndex,
		ConfState: n.confState.Clone(),
		Progress:  n.progress,
	}
}

// bootstrapPeers builds the bootstrap membership in a deterministic order
// with identical contexts on every node, so that every founding member
// appends byte-for-byte identical entries at indexes 1..N. Client addresses
// are published separately once the cluster runs.
func bootstrapPeers(initial map[raft.NodeID]string) []raft.Peer {
	ids := make([]raft.NodeID, 0, len(initial))
	for id := range initial {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	peers := make([]raft.Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, raft.Peer{ID: id, Context: encodeMemberContext(memberContext{RaftAddress: initial[id]})})
	}
	return peers
}
