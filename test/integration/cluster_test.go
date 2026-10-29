// Package integration starts real kvorum nodes in-process, connected over
// loopback gRPC, and exercises the system end to end: replication, leader
// failover, membership changes, snapshots, linearizable reads, and
// exactly-once retries.
package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/client"
	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/internal/server"
	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/raft"
)

const (
	tickInterval = 20 * time.Millisecond
	electionTick = 10
)

func testLogger(tb testing.TB) *slog.Logger {
	tb.Helper()
	if os.Getenv("KVORUM_TEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// member is one running node with its server and a gRPC connection.
type member struct {
	id         raft.NodeID
	dataDir    string
	raftAddr   string
	clientAddr string
	node       *node.Node
	server     *server.Server
	conn       *grpc.ClientConn
	kv         kvorumpb.KVServiceClient
	admin      kvorumpb.AdminServiceClient
}

// cluster manages a set of in-process members.
type cluster struct {
	tb      testing.TB
	root    string
	mu      sync.Mutex
	members map[raft.NodeID]*member
	raft    map[raft.NodeID]string // reserved raft addresses by id
	client  map[raft.NodeID]string
	opts    clusterOptions
}

type clusterOptions struct {
	snapshotThreshold uint64
}

// reserveAddr returns a free loopback address that can be re-bound later
// (the listener is closed again immediately).
func reserveAddr(tb testing.TB) string {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func newCluster(tb testing.TB, size int, opts clusterOptions) *cluster {
	tb.Helper()
	c := &cluster{
		tb:      tb,
		root:    tb.TempDir(),
		members: make(map[raft.NodeID]*member),
		raft:    make(map[raft.NodeID]string),
		client:  make(map[raft.NodeID]string),
		opts:    opts,
	}
	initial := make(map[raft.NodeID]string, size)
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		c.raft[id] = reserveAddr(tb)
		c.client[id] = reserveAddr(tb)
		initial[id] = c.raft[id]
	}
	for i := 1; i <= size; i++ {
		c.start(raft.NodeID(i), initial, false)
	}
	tb.Cleanup(c.stopAll)
	return c
}

func (c *cluster) start(id raft.NodeID, initial map[raft.NodeID]string, join bool) *member {
	c.tb.Helper()
	m := &member{
		id:         id,
		dataDir:    filepath.Join(c.root, fmt.Sprintf("node%d", id)),
		raftAddr:   c.raft[id],
		clientAddr: c.client[id],
	}
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry)
	threshold := c.opts.snapshotThreshold
	n, err := node.Open(node.Config{
		ID:                     id,
		DataDir:                m.dataDir,
		ListenRaft:             m.raftAddr,
		AdvertiseRaft:          m.raftAddr,
		AdvertiseClient:        m.clientAddr,
		InitialCluster:         initial,
		Join:                   join,
		TickInterval:           tickInterval,
		ElectionTick:           electionTick,
		HeartbeatTick:          1,
		SnapshotThreshold:      threshold,
		SnapshotCatchUpEntries: 2,
		Logger:                 testLogger(c.tb).With("test_node", id),
		Metrics:                metrics,
	})
	if err != nil {
		c.tb.Fatalf("open node %d: %v", id, err)
	}
	if err := n.Start(); err != nil {
		c.tb.Fatalf("start node %d: %v", id, err)
	}
	srv, err := server.New(server.Config{
		ListenClient: m.clientAddr,
		Node:         n,
		Metrics:      metrics,
		Gatherer:     registry,
		Logger:       testLogger(c.tb),
	})
	if err != nil {
		c.tb.Fatalf("new server %d: %v", id, err)
	}
	if err := srv.Start(); err != nil {
		c.tb.Fatalf("start server %d: %v", id, err)
	}
	conn, err := grpc.NewClient(m.clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.tb.Fatalf("dial %d: %v", id, err)
	}
	m.node, m.server, m.conn = n, srv, conn
	m.kv = kvorumpb.NewKVServiceClient(conn)
	m.admin = kvorumpb.NewAdminServiceClient(conn)
	c.mu.Lock()
	c.members[id] = m
	c.mu.Unlock()
	return m
}

func (c *cluster) stop(id raft.NodeID) {
	c.tb.Helper()
	c.mu.Lock()
	m, ok := c.members[id]
	delete(c.members, id)
	c.mu.Unlock()
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = m.conn.Close()
	if err := m.server.Stop(ctx); err != nil {
		c.tb.Logf("stop server %d: %v", id, err)
	}
	if err := m.node.Stop(ctx); err != nil {
		c.tb.Logf("stop node %d: %v", id, err)
	}
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	ids := make([]raft.NodeID, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.stop(id)
	}
}

func (c *cluster) live() []*member {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*member, 0, len(c.members))
	for id := raft.NodeID(1); id <= raft.NodeID(len(c.members)+8); id++ {
		if m, ok := c.members[id]; ok {
			out = append(out, m)
		}
	}
	return out
}

// waitLeader waits until some live member reports itself as leader and
// returns it.
func (c *cluster) waitLeader() *member {
	c.tb.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range c.live() {
			if m.node.IsLeader() {
				return m
			}
		}
		time.Sleep(tickInterval)
	}
	c.tb.Fatal("no leader elected")
	return nil
}

// put writes through whichever member is leader, following redirects.
func (c *cluster) put(ctx context.Context, key, value string) *kvorumpb.PutResponse {
	c.tb.Helper()
	var last error
	for range 50 {
		for _, m := range c.live() {
			resp, err := m.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte(key), Value: []byte(value)})
			if err == nil {
				return resp
			}
			last = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.tb.Fatalf("put %s failed: %v", key, last)
	return nil
}

func (c *cluster) get(ctx context.Context, m *member, key string, consistency kvorumpb.ReadConsistency) (*kvorumpb.GetResponse, error) {
	return m.kv.Get(ctx, &kvorumpb.GetRequest{Key: []byte(key), Consistency: consistency})
}

// eventually retries fn until it returns nil or the timeout passes.
func eventually(tb testing.TB, timeout time.Duration, fn func() error) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("condition not met within %s: %v", timeout, err)
}

func leaderHint(err error) (*kvorumpb.LeaderHint, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	for _, d := range st.Details() {
		if h, ok := d.(*kvorumpb.LeaderHint); ok {
			return h, true
		}
	}
	return nil, false
}

func TestReplicationAndLinearizableReads(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lead := c.waitLeader()

	for i := range 20 {
		c.put(ctx, fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i))
	}
	// Linearizable reads work on every node, including followers, and see
	// every acknowledged write.
	for _, m := range c.live() {
		resp, err := c.get(ctx, m, "key-19", kvorumpb.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE)
		if err != nil {
			t.Fatalf("linearizable get on %d: %v", m.id, err)
		}
		if !resp.Found || string(resp.Kv.Value) != "value-19" {
			t.Fatalf("node %d returned %v", m.id, resp)
		}
		if resp.Header.MemberId != uint64(m.id) || resp.Header.Revision == 0 {
			t.Fatalf("node %d header = %v", m.id, resp.Header)
		}
	}
	// Stale reads eventually converge.
	for _, m := range c.live() {
		eventually(t, 5*time.Second, func() error {
			resp, err := c.get(ctx, m, "key-00", kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE)
			if err != nil {
				return err
			}
			if !resp.Found {
				return errors.New("not found yet")
			}
			return nil
		})
	}
	// Scan with a limit reports more.
	scan, err := lead.kv.Scan(ctx, &kvorumpb.ScanRequest{Start: []byte("key-"), End: []byte("key."), Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Kvs) != 5 || !scan.More || string(scan.Kvs[0].Key) != "key-00" {
		t.Fatalf("scan = %d keys, more=%t, first=%q", len(scan.Kvs), scan.More, scan.Kvs[0].Key)
	}
	// Followers reject writes with a leader hint.
	for _, m := range c.live() {
		if m == lead {
			continue
		}
		_, err := m.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte("x"), Value: []byte("y")})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("follower %d put: code = %v (%v)", m.id, status.Code(err), err)
		}
		hint, ok := leaderHint(err)
		if !ok || hint.LeaderId != uint64(lead.id) || hint.LeaderAddress != lead.clientAddr {
			t.Fatalf("follower %d hint = %v (want leader %d at %s)", m.id, hint, lead.id, lead.clientAddr)
		}
	}
	// Status agrees across nodes.
	for _, m := range c.live() {
		st, err := m.admin.Status(ctx, &kvorumpb.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if st.Leader != uint64(lead.id) || len(st.Members) != 3 {
			t.Fatalf("status on %d = %v", m.id, st)
		}
	}
}

func TestExactlyOnceRetries(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lead := c.waitLeader()

	sess := &kvorumpb.Session{ClientId: 42, Sequence: 1}
	first, err := lead.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte("counter"), Value: []byte("1"), Session: sess})
	if err != nil {
		t.Fatal(err)
	}
	// A retry with the same session is not applied twice: the version does
	// not change and the recorded result comes back.
	again, err := lead.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte("counter"), Value: []byte("1"), Session: sess})
	if err != nil {
		t.Fatal(err)
	}
	if again.Kv.Version != first.Kv.Version || again.Kv.ModRevision != first.Kv.ModRevision {
		t.Fatalf("retry changed the key: %v vs %v", again.Kv, first.Kv)
	}
	// A new sequence applies.
	next, err := lead.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte("counter"), Value: []byte("2"), Session: &kvorumpb.Session{ClientId: 42, Sequence: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if next.Kv.Version != 2 {
		t.Fatalf("version after second write = %d", next.Kv.Version)
	}
	// A stale sequence is refused.
	_, err = lead.kv.Put(ctx, &kvorumpb.PutRequest{Key: []byte("counter"), Value: []byte("0"), Session: &kvorumpb.Session{ClientId: 42, Sequence: 1}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale sequence: code = %v", status.Code(err))
	}

	// Compare-and-swap semantics.
	cas, err := lead.kv.CompareAndSwap(ctx, &kvorumpb.CompareAndSwapRequest{Key: []byte("counter"), ExpectedVersion: 1, Value: []byte("x")})
	if err != nil || cas.Succeeded {
		t.Fatalf("cas with wrong version succeeded: %v, %v", cas, err)
	}
	cas, err = lead.kv.CompareAndSwap(ctx, &kvorumpb.CompareAndSwapRequest{Key: []byte("counter"), ExpectedVersion: 2, Value: []byte("3")})
	if err != nil || !cas.Succeeded || string(cas.Kv.Value) != "3" {
		t.Fatalf("cas with right version failed: %v, %v", cas, err)
	}
	cas, err = lead.kv.CompareAndSwap(ctx, &kvorumpb.CompareAndSwapRequest{Key: []byte("fresh"), ExpectedVersion: 0, Value: []byte("created")})
	if err != nil || !cas.Succeeded {
		t.Fatalf("create-if-absent failed: %v, %v", cas, err)
	}
	del, err := lead.kv.Delete(ctx, &kvorumpb.DeleteRequest{Key: []byte("fresh"), PrevKv: true})
	if err != nil || !del.Deleted || string(del.PrevKv.Value) != "created" {
		t.Fatalf("delete = %v, %v", del, err)
	}
}

func TestLeaderFailoverAndRestart(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := c.waitLeader()
	c.put(ctx, "before", "crash")

	// Kill the leader; the others elect a new one and keep serving.
	oldLeader := lead.id
	c.stop(oldLeader)
	newLeader := c.waitLeader()
	if newLeader.id == oldLeader {
		t.Fatalf("stopped node %d is still leader", oldLeader)
	}
	c.put(ctx, "after", "failover")
	resp, err := c.get(ctx, newLeader, "before", kvorumpb.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE)
	if err != nil || !resp.Found || string(resp.Kv.Value) != "crash" {
		t.Fatalf("data lost across failover: %v, %v", resp, err)
	}

	// Restart the old leader from its data directory; it catches up.
	initial := make(map[raft.NodeID]string)
	for id, addr := range c.raft {
		initial[id] = addr
	}
	restarted := c.start(oldLeader, initial, false)
	eventually(t, 10*time.Second, func() error {
		r, err := c.get(ctx, restarted, "after", kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE)
		if err != nil {
			return err
		}
		if !r.Found {
			return errors.New("restarted node has not caught up")
		}
		return nil
	})
	st, err := restarted.admin.Status(ctx, &kvorumpb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.State == "leader" && st.Term == 0 {
		t.Fatalf("restarted status = %v", st)
	}
}

func TestMembershipChangesAndSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{snapshotThreshold: 10})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lead := c.waitLeader()

	// Enough writes to force snapshots and compaction on every node.
	for i := range 60 {
		c.put(ctx, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i))
	}
	eventually(t, 10*time.Second, func() error {
		st, err := lead.admin.Status(ctx, &kvorumpb.StatusRequest{})
		if err != nil {
			return err
		}
		if st.SnapshotIndex == 0 {
			return errors.New("no snapshot yet")
		}
		return nil
	})

	// Add node 4 as a learner: it must catch up through a snapshot.
	c.raft[4] = reserveAddr(t)
	c.client[4] = reserveAddr(t)
	existing := make(map[raft.NodeID]string)
	for id := raft.NodeID(1); id <= 3; id++ {
		existing[id] = c.raft[id]
	}
	joiner := c.start(4, existing, true)
	if _, err := lead.admin.AddMember(ctx, &kvorumpb.AddMemberRequest{Member: &kvorumpb.Member{
		Id: 4, RaftAddress: c.raft[4], ClientAddress: c.client[4], Learner: true,
	}}); err != nil {
		t.Fatalf("add learner: %v", err)
	}
	eventually(t, 20*time.Second, func() error {
		r, err := c.get(ctx, joiner, "k059", kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE)
		if err != nil {
			return err
		}
		if !r.Found {
			return errors.New("learner has not caught up")
		}
		return nil
	})
	st, err := joiner.admin.Status(ctx, &kvorumpb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.SnapshotIndex == 0 {
		t.Fatalf("learner did not install a snapshot: %v", st)
	}
	members, err := lead.admin.ListMembers(ctx, &kvorumpb.ListMembersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var learner *kvorumpb.Member
	for _, m := range members.Members {
		if m.Id == 4 {
			learner = m
		}
	}
	if learner == nil || !learner.Learner || learner.ClientAddress != c.client[4] {
		t.Fatalf("members = %v", members.Members)
	}

	// Promote, then remove the original leader; the cluster keeps working.
	if _, err := lead.admin.PromoteMember(ctx, &kvorumpb.PromoteMemberRequest{Id: 4}); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := lead.admin.RemoveMember(ctx, &kvorumpb.RemoveMemberRequest{Id: uint64(lead.id)}); err != nil {
		t.Fatalf("remove leader: %v", err)
	}
	select {
	case <-lead.node.Removed():
	case <-time.After(10 * time.Second):
		t.Fatal("removed node did not observe its removal")
	}
	c.stop(lead.id)
	newLeader := c.waitLeader()
	c.put(ctx, "after-removal", "ok")
	members, err = newLeader.admin.ListMembers(ctx, &kvorumpb.ListMembersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(members.Members) != 3 {
		t.Fatalf("members after removal = %v", members.Members)
	}
	for _, m := range members.Members {
		if m.Id == uint64(lead.id) || m.Learner {
			t.Fatalf("unexpected member %v", m)
		}
	}
}

func TestLeadershipTransferAndWatch(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := c.waitLeader()

	// Watch on a follower sees writes made through the leader.
	var follower *member
	for _, m := range c.live() {
		if m != lead {
			follower = m
			break
		}
	}
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	stream, err := follower.kv.Watch(wctx, &kvorumpb.WatchRequest{Key: []byte("watched/"), RangeEnd: []byte("watched0")})
	if err != nil {
		t.Fatal(err)
	}
	// Give the watch a moment to register before writing.
	time.Sleep(100 * time.Millisecond)
	c.put(ctx, "watched/a", "1")
	c.put(ctx, "watched/b", "2")
	if _, err := lead.kv.Delete(ctx, &kvorumpb.DeleteRequest{Key: []byte("watched/a")}); err != nil {
		t.Fatal(err)
	}
	var events []*kvorumpb.Event
	for len(events) < 3 {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("watch recv: %v", err)
		}
		events = append(events, resp.Events...)
	}
	if events[0].Type != kvorumpb.Event_TYPE_PUT || string(events[0].Kv.Key) != "watched/a" ||
		events[1].Type != kvorumpb.Event_TYPE_PUT || string(events[1].Kv.Key) != "watched/b" ||
		events[2].Type != kvorumpb.Event_TYPE_DELETE || string(events[2].Kv.Key) != "watched/a" {
		t.Fatalf("events = %v", events)
	}
	wcancel()

	// Transfer leadership to the follower.
	if _, err := lead.admin.TransferLeadership(ctx, &kvorumpb.TransferLeadershipRequest{TargetId: uint64(follower.id)}); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	eventually(t, 10*time.Second, func() error {
		if !follower.node.IsLeader() {
			return fmt.Errorf("node %d is not leader yet", follower.id)
		}
		return nil
	})
	c.put(ctx, "after-transfer", "ok")

	// A forced snapshot reports its index.
	snap, err := follower.admin.Snapshot(ctx, &kvorumpb.SnapshotRequest{})
	if err != nil || snap.SnapshotIndex == 0 {
		t.Fatalf("snapshot = %v, %v", snap, err)
	}
}

func TestGracefulStopHandsOverLeadership(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := c.waitLeader()
	c.put(ctx, "k", "v")

	start := time.Now()
	c.stop(lead.id)
	newLeader := c.waitLeader()
	elapsed := time.Since(start)
	if newLeader.id == lead.id {
		t.Fatal("stopped node still leader")
	}
	// A handover is much faster than waiting for an election timeout
	// (electionTick*tickInterval = 200ms plus randomization); allow slack
	// for slow CI machines but make sure it is not pathological.
	if elapsed > 5*time.Second {
		t.Fatalf("failover after graceful stop took %s", elapsed)
	}
	c.put(ctx, "k2", "v2")
}

func TestClientConcurrentWritesAndPublishedAddresses(t *testing.T) {
	c := newCluster(t, 3, clusterOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lead := c.waitLeader()

	// Every member publishes its client address, leader and followers alike.
	eventually(t, 10*time.Second, func() error {
		members, err := lead.admin.ListMembers(ctx, &kvorumpb.ListMembersRequest{})
		if err != nil {
			return err
		}
		for _, m := range members.Members {
			if m.ClientAddress != c.client[raft.NodeID(m.Id)] {
				return fmt.Errorf("member %d has client address %q", m.Id, m.ClientAddress)
			}
		}
		return nil
	})

	// Concurrent writes through one client must all succeed: each in-flight
	// write uses its own exactly-once session.
	endpoints := make([]string, 0, 3)
	for id := raft.NodeID(1); id <= 3; id++ {
		endpoints = append(endpoints, c.client[id])
	}
	cl, err := client.New(client.Config{Endpoints: endpoints, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	const workers, perWorker = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				key := fmt.Appendf(nil, "concurrent/%d/%d", w, i)
				if _, err := cl.Put(ctx, key, []byte("v")); err != nil {
					errs <- fmt.Errorf("worker %d write %d: %w", w, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	res, err := cl.Scan(ctx, []byte("concurrent/"), nil, client.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != workers*perWorker {
		t.Fatalf("scan found %d keys, want %d", len(res.KVs), workers*perWorker)
	}
}
