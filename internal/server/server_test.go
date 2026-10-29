package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/raft"
)

func reserveAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startSingleNode runs a one-member cluster with a server in front of it.
func startSingleNode(t *testing.T) (*Server, *node.Node, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry)
	raftAddr := reserveAddr(t)
	clientAddr := reserveAddr(t)
	n, err := node.Open(node.Config{
		ID:              1,
		DataDir:         t.TempDir(),
		ListenRaft:      raftAddr,
		AdvertiseClient: clientAddr,
		InitialCluster:  map[raft.NodeID]string{1: raftAddr},
		TickInterval:    10 * time.Millisecond,
		ElectionTick:    5,
		Metrics:         metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{
		ListenClient: clientAddr,
		ListenHTTP:   reserveAddr(t),
		Node:         n,
		Metrics:      metrics,
		Gatherer:     registry,
		MaxScanLimit: 3,
		WatchBuffer:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
		_ = n.Stop(ctx)
	})
	deadline := time.Now().Add(5 * time.Second)
	for !n.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("single node did not become leader")
	}
	return s, n, registry
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config accepted")
	}
	if _, err := New(Config{ListenClient: "127.0.0.1:0"}); err == nil {
		t.Fatal("config without node accepted")
	}
}

func TestKVServiceEndToEnd(t *testing.T) {
	s, _, registry := startSingleNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn := dial(t, s.ClientAddress())
	kvc := kvorumpb.NewKVServiceClient(conn)
	admin := kvorumpb.NewAdminServiceClient(conn)

	// Validation.
	if _, err := kvc.Put(ctx, &kvorumpb.PutRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty key: %v", err)
	}
	if _, err := kvc.Scan(ctx, &kvorumpb.ScanRequest{Limit: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative limit: %v", err)
	}
	if _, err := kvc.Put(ctx, &kvorumpb.PutRequest{Key: []byte("k"), Value: make([]byte, kv.MaxValueSize+1)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized value: %v", err)
	}

	// Writes and reads.
	for i := range 5 {
		if _, err := kvc.Put(ctx, &kvorumpb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	get, err := kvc.Get(ctx, &kvorumpb.GetRequest{Key: []byte("k1")})
	if err != nil || !get.Found || string(get.Kv.Value) != "v" || get.Header.MemberId != 1 {
		t.Fatalf("get = %v, %v", get, err)
	}
	missing, err := kvc.Get(ctx, &kvorumpb.GetRequest{Key: []byte("nope"), Consistency: kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE})
	if err != nil || missing.Found {
		t.Fatalf("missing get = %v, %v", missing, err)
	}
	scan, err := kvc.Scan(ctx, &kvorumpb.ScanRequest{Start: []byte("k"), End: []byte("l")})
	if err != nil || len(scan.Kvs) != 3 || !scan.More {
		t.Fatalf("scan with server limit = %v, %v", scan, err)
	}

	// Watch delivers events and reports overflow when the client is slow.
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	stream, err := kvc.Watch(wctx, &kvorumpb.WatchRequest{Key: []byte("w")})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := kvc.Put(ctx, &kvorumpb.PutRequest{Key: []byte("w"), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil || len(resp.Events) != 1 || resp.Events[0].Type != kvorumpb.Event_TYPE_PUT {
		t.Fatalf("watch = %v, %v", resp, err)
	}
	wcancel()

	// Admin surface.
	st, err := admin.Status(ctx, &kvorumpb.StatusRequest{})
	if err != nil || st.State != "leader" || len(st.Members) != 1 || st.Members[0].ClientAddress != s.ClientAddress() {
		t.Fatalf("status = %v, %v", st, err)
	}
	if _, err := admin.AddMember(ctx, &kvorumpb.AddMemberRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("add member without data: %v", err)
	}
	// Removing a node that is not a member is idempotent; promoting one is
	// an error because there is nothing to promote.
	if _, err := admin.RemoveMember(ctx, &kvorumpb.RemoveMemberRequest{Id: 9}); err != nil {
		t.Fatalf("remove unknown member: %v", err)
	}
	if _, err := admin.PromoteMember(ctx, &kvorumpb.PromoteMemberRequest{Id: 9}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("promote unknown member: %v", err)
	}
	if _, err := admin.TransferLeadership(ctx, &kvorumpb.TransferLeadershipRequest{TargetId: 9}); status.Code(err) != codes.NotFound {
		t.Fatalf("transfer to unknown: %v", err)
	}
	snap, err := admin.Snapshot(ctx, &kvorumpb.SnapshotRequest{})
	if err != nil || snap.SnapshotIndex == 0 {
		t.Fatalf("snapshot = %v, %v", snap, err)
	}

	// HTTP endpoints.
	base := "http://" + s.HTTPAddress()
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/healthz", http.StatusOK, "ok"},
		{"/readyz", http.StatusOK, "ready"},
		{"/status", http.StatusOK, `"state": "leader"`},
		{"/metrics", http.StatusOK, "kvorum_raft_is_leader 1"},
		{"/debug/pprof/", http.StatusOK, "goroutine"},
	} {
		res, err := http.Get(base + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != tc.code || !strings.Contains(string(body), tc.body) {
			t.Fatalf("GET %s = %d %q", tc.path, res.StatusCode, body)
		}
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "kvorum_kv_requests_total" {
			found = true
		}
	}
	if !found {
		t.Fatal("request metrics were not recorded")
	}
}

func TestToStatusMapping(t *testing.T) {
	s, _, _ := startSingleNode(t)
	cases := []struct {
		err  error
		code codes.Code
	}{
		{nil, codes.OK},
		{node.ErrNotLeader, codes.Unavailable},
		{node.ErrNoLeader, codes.Unavailable},
		{node.ErrProposalLost, codes.Unavailable},
		{node.ErrLeadershipTransferring, codes.Unavailable},
		{node.ErrStopped, codes.Unavailable},
		{raft.ErrConfChangePending, codes.Unavailable},
		{node.ErrQueueFull, codes.ResourceExhausted},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{context.Canceled, codes.Canceled},
		{fmt.Errorf("wrapped: %w", kv.ErrInvalidCommand), codes.InvalidArgument},
		{node.ErrInvalidMember, codes.InvalidArgument},
		{kv.ErrStaleSequence, codes.FailedPrecondition},
		{raft.ErrInvalidConfChange, codes.FailedPrecondition},
		{raft.ErrUnknownNode, codes.NotFound},
		{kv.ErrWatcherOverflow, codes.ResourceExhausted},
		{errors.New("boom"), codes.Internal},
		{status.Error(codes.Aborted, "passthrough"), codes.Aborted},
	}
	for _, tc := range cases {
		if got := status.Code(s.toStatus(tc.err)); got != tc.code {
			t.Errorf("toStatus(%v) = %v, want %v", tc.err, got, tc.code)
		}
	}
	// Leadership errors carry a hint with the leader's address.
	st, _ := status.FromError(s.toStatus(node.ErrNotLeader))
	hinted := false
	for _, d := range st.Details() {
		if h, ok := d.(*kvorumpb.LeaderHint); ok && h.LeaderId == 1 && h.LeaderAddress == s.ClientAddress() {
			hinted = true
		}
	}
	if !hinted {
		t.Fatalf("no leader hint in %v", st.Details())
	}
	// A transfer in progress must not point clients back at this node.
	st, _ = status.FromError(s.toStatus(node.ErrLeadershipTransferring))
	if len(st.Details()) != 0 {
		t.Fatalf("transfer error carried a hint: %v", st.Details())
	}
}

func TestHelpers(t *testing.T) {
	t.Parallel()
	if methodName("/kvorum.v1.KVService/Put") != "Put" || methodName("Put") != "Put" {
		t.Fatal("methodName")
	}
	if toProtoKV(nil) != nil {
		t.Fatal("nil kv should map to nil")
	}
	ev := toProtoEvent(kv.Event{Type: kv.EventDelete, KV: kv.KeyValue{Key: []byte("k")}})
	if ev.Type != kvorumpb.Event_TYPE_DELETE || string(ev.Kv.Key) != "k" || ev.PrevKv != nil {
		t.Fatalf("event = %v", ev)
	}
	ctx, cancel := requestDeadline(context.Background(), time.Second)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("deadline not applied")
	}
	parent, pcancel := context.WithTimeout(context.Background(), time.Hour)
	defer pcancel()
	ctx2, cancel2 := requestDeadline(parent, time.Second)
	defer cancel2()
	if d, _ := ctx2.Deadline(); time.Until(d) < 30*time.Minute {
		t.Fatal("existing deadline was shortened")
	}
	if err := validateKey(make([]byte, kv.MaxKeySize+1)); status.Code(err) != codes.InvalidArgument {
		t.Fatal("oversized key accepted")
	}
}
