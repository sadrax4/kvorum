package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testTimeout = 10 * time.Second

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func fastConfig(endpoints ...string) Config {
	return Config{
		Endpoints:        endpoints,
		DialTimeout:      time.Second,
		RequestTimeout:   2 * time.Second,
		MaxRetries:       4,
		RetryBaseBackoff: time.Millisecond,
		RetryMaxBackoff:  5 * time.Millisecond,
	}
}

func newTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// deadAddress returns an address nothing listens on.
func deadAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	t.Run("no endpoints", func(t *testing.T) {
		t.Parallel()
		if _, err := New(Config{}); !errors.Is(err, ErrNoEndpoints) {
			t.Fatalf("New() error = %v, want ErrNoEndpoints", err)
		}
		if _, err := New(Config{Endpoints: []string{" ", ""}}); !errors.Is(err, ErrNoEndpoints) {
			t.Fatalf("New(blank) error = %v, want ErrNoEndpoints", err)
		}
	})
	t.Run("defaults", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t, Config{Endpoints: []string{"127.0.0.1:1", " 127.0.0.1:1 ", "127.0.0.1:2"}})
		if c.cfg.DialTimeout != DefaultDialTimeout || c.cfg.RequestTimeout != DefaultRequestTimeout ||
			c.cfg.MaxRetries != DefaultMaxRetries || c.cfg.RetryBaseBackoff != DefaultRetryBaseBackoff ||
			c.cfg.RetryMaxBackoff != DefaultRetryMaxBackoff {
			t.Fatalf("defaults not applied: %+v", c.cfg)
		}
		if c.ClientID() == 0 {
			t.Fatal("ClientID should be drawn at random when zero")
		}
		if c.cfg.Logger == nil {
			t.Fatal("logger should default to a discarding logger")
		}
		if got := c.endpoints.all(); len(got) != 2 {
			t.Fatalf("endpoints should be trimmed and deduplicated, got %q", got)
		}
	})
	t.Run("explicit client id", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t, Config{Endpoints: []string{"127.0.0.1:1"}, ClientID: 42, MaxRetries: -1})
		if c.ClientID() != 42 {
			t.Fatalf("ClientID = %d, want 42", c.ClientID())
		}
		if c.cfg.MaxRetries != 0 {
			t.Fatalf("negative MaxRetries should become 0, got %d", c.cfg.MaxRetries)
		}
	})
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)

	if _, err := c.Get(ctx, []byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(absent) error = %v, want ErrNotFound", err)
	}

	put, err := c.Put(ctx, []byte("k"), []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.KV == nil || put.KV.Version != 1 || !bytes.Equal(put.KV.Value, []byte("v1")) {
		t.Fatalf("unexpected put result: %+v", put.KV)
	}
	if put.PrevKV != nil {
		t.Fatalf("PrevKV should be nil without WithPrevKV, got %+v", put.PrevKV)
	}
	if put.Header.MemberID != 1 || put.Header.Revision == 0 {
		t.Fatalf("unexpected header: %+v", put.Header)
	}

	put2, err := c.Put(ctx, []byte("k"), []byte("v2"), WithPrevKV())
	if err != nil {
		t.Fatalf("Put(prevkv): %v", err)
	}
	if put2.PrevKV == nil || !bytes.Equal(put2.PrevKV.Value, []byte("v1")) || put2.KV.Version != 2 {
		t.Fatalf("unexpected second put result: kv=%+v prev=%+v", put2.KV, put2.PrevKV)
	}

	kv, err := c.Get(ctx, []byte("k"), WithStale())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(kv.Value, []byte("v2")) || kv.Version != 2 || kv.CreateRevision != put.Header.Revision {
		t.Fatalf("unexpected kv: %+v", kv)
	}
	if srv.callCount("Get.stale") != 1 {
		t.Fatalf("WithStale should set READ_CONSISTENCY_STALE, stale gets = %d", srv.callCount("Get.stale"))
	}

	del, err := c.Delete(ctx, []byte("k"), WithPrevKV())
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !del.Deleted || del.PrevKV == nil || !bytes.Equal(del.PrevKV.Value, []byte("v2")) {
		t.Fatalf("unexpected delete result: %+v", del)
	}
	del2, err := c.Delete(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Delete(absent): %v", err)
	}
	if del2.Deleted {
		t.Fatal("deleting an absent key should report Deleted=false")
	}
}

func TestCompareAndSwap(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)

	res, err := c.CompareAndSwap(ctx, []byte("lock"), 0, []byte("me"))
	if err != nil {
		t.Fatalf("CAS(create): %v", err)
	}
	if !res.Succeeded || res.KV == nil || res.KV.Version != 1 {
		t.Fatalf("CAS(create) should succeed: %+v", res)
	}
	res, err = c.CompareAndSwap(ctx, []byte("lock"), 0, []byte("you"))
	if err != nil {
		t.Fatalf("CAS(conflict): %v", err)
	}
	if res.Succeeded || res.KV == nil || !bytes.Equal(res.KV.Value, []byte("me")) {
		t.Fatalf("CAS(conflict) should fail and return the current value: %+v", res)
	}
	res, err = c.CompareAndSwap(ctx, []byte("lock"), 1, []byte("you"))
	if err != nil {
		t.Fatalf("CAS(match): %v", err)
	}
	if !res.Succeeded || res.KV.Version != 2 {
		t.Fatalf("CAS(match) should succeed: %+v", res)
	}
}

func TestScanPrefixAndLimit(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1", "a0"} {
		if _, err := c.Put(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	res, err := c.Scan(ctx, []byte("a/"), []byte("zzz"), WithPrefix())
	if err != nil {
		t.Fatalf("Scan(prefix): %v", err)
	}
	if len(res.KVs) != 3 || res.More {
		t.Fatalf("prefix scan should return exactly a/1..a/3, got %d keys more=%v", len(res.KVs), res.More)
	}
	for i, want := range []string{"a/1", "a/2", "a/3"} {
		if string(res.KVs[i].Key) != want {
			t.Fatalf("KVs[%d].Key = %q, want %q", i, res.KVs[i].Key, want)
		}
	}

	res, err = c.Scan(ctx, []byte("a/"), nil, WithPrefix(), WithLimit(2))
	if err != nil {
		t.Fatalf("Scan(limit): %v", err)
	}
	if len(res.KVs) != 2 || !res.More {
		t.Fatalf("limited scan should return 2 keys and More=true, got %d more=%v", len(res.KVs), res.More)
	}

	res, err = c.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Scan(all): %v", err)
	}
	if len(res.KVs) != 5 {
		t.Fatalf("unbounded scan should return every key, got %d", len(res.KVs))
	}

	res, err = c.Scan(ctx, []byte("a/2"), []byte("b/1"))
	if err != nil {
		t.Fatalf("Scan(range): %v", err)
	}
	if len(res.KVs) != 3 || string(res.KVs[2].Key) != "a0" {
		t.Fatalf("range scan [a/2, b/1) should return a/2, a/3, a0; got %d keys", len(res.KVs))
	}
}

func TestLeaderRedirect(t *testing.T) {
	t.Parallel()
	follower := startFakeServer(t, 1)
	leader := startFakeServer(t, 2)
	follower.setLeader(leader.addr)
	c := newTestClient(t, fastConfig(follower.addr))
	ctx := testContext(t)

	put, err := c.Put(ctx, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Header.MemberID != 2 {
		t.Fatalf("write should have been served by the leader, got member %d", put.Header.MemberID)
	}
	if follower.callCount("Put") != 1 || leader.callCount("Put") != 1 {
		t.Fatalf("expected one attempt per node, follower=%d leader=%d", follower.callCount("Put"), leader.callCount("Put"))
	}
	if got := c.endpoints.current(); got != leader.addr {
		t.Fatalf("preferred endpoint = %s, want leader %s", got, leader.addr)
	}

	// Later requests go straight to the leader.
	if _, err := c.Get(ctx, []byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if follower.callCount("Get") != 0 || leader.callCount("Get") != 1 {
		t.Fatalf("follow-up read should go to the leader directly, follower=%d leader=%d", follower.callCount("Get"), leader.callCount("Get"))
	}
}

func TestLeaderRedirectFailedPrecondition(t *testing.T) {
	t.Parallel()
	follower := startFakeServer(t, 1)
	leader := startFakeServer(t, 2)
	follower.setLeader(leader.addr)
	follower.setLeaderHintCode(codes.FailedPrecondition)
	c := newTestClient(t, fastConfig(follower.addr))

	if _, err := c.Put(testContext(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if leader.callCount("Put") != 1 {
		t.Fatalf("FailedPrecondition with a hint should be followed, leader puts = %d", leader.callCount("Put"))
	}
}

func TestLeaderRedirectPingPongIsBounded(t *testing.T) {
	t.Parallel()
	a := startFakeServer(t, 1)
	b := startFakeServer(t, 2)
	a.setLeader(b.addr)
	b.setLeader(a.addr)
	cfg := fastConfig(a.addr)
	cfg.MaxRetries = 3
	c := newTestClient(t, cfg)

	_, err := c.Get(testContext(t), []byte("k"))
	if err == nil {
		t.Fatal("Get should fail when nodes point at each other")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error code = %v, want Unavailable; err = %v", status.Code(err), err)
	}
	if total := a.callCount("Get") + b.callCount("Get"); total != cfg.MaxRetries+1 {
		t.Fatalf("total attempts = %d, want %d", total, cfg.MaxRetries+1)
	}
}

func TestRetryPreservesSession(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.failNextWith(2, status.Error(codes.Unavailable, "proposal dropped"))
	cfg := fastConfig(srv.addr)
	cfg.ClientID = 1234
	c := newTestClient(t, cfg)

	if _, err := c.Put(testContext(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	sessions := srv.recordedSessions()
	if len(sessions) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(sessions))
	}
	for i, s := range sessions {
		if s.GetClientId() != 1234 {
			t.Fatalf("attempt %d client id = %d, want 1234", i, s.GetClientId())
		}
		if s.GetSequence() != sessions[0].GetSequence() {
			t.Fatalf("attempt %d sequence = %d, want %d (same as first attempt)", i, s.GetSequence(), sessions[0].GetSequence())
		}
	}
	if sessions[0].GetSequence() == 0 {
		t.Fatal("sequence numbers start at 1")
	}

	// A new logical write gets a new sequence.
	if _, err := c.Delete(testContext(t), []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	sessions = srv.recordedSessions()
	if last := sessions[len(sessions)-1]; last.GetSequence() != sessions[0].GetSequence()+1 {
		t.Fatalf("next write sequence = %d, want %d", last.GetSequence(), sessions[0].GetSequence()+1)
	}
}

func TestRetryAbortedAndDeadline(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.failNextWith(1, status.Error(codes.Aborted, "leadership changed"))
	c := newTestClient(t, fastConfig(srv.addr))
	if _, err := c.Put(testContext(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put after Aborted: %v", err)
	}
	if srv.callCount("Put") != 2 {
		t.Fatalf("Aborted should be retried once, puts = %d", srv.callCount("Put"))
	}
}

func TestNonRetryableErrorsReturnImmediately(t *testing.T) {
	t.Parallel()
	nonRetryable := []codes.Code{
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Internal,
	}
	for _, code := range nonRetryable {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			srv := startFakeServer(t, 1)
			srv.failAlwaysWith(status.Error(code, "nope"))
			c := newTestClient(t, fastConfig(srv.addr))
			_, err := c.Put(testContext(t), []byte("k"), []byte("v"))
			if status.Code(err) != code {
				t.Fatalf("error code = %v, want %v (err %v)", status.Code(err), code, err)
			}
			if srv.callCount("Put") != 1 {
				t.Fatalf("%v must not be retried, attempts = %d", code, srv.callCount("Put"))
			}
		})
	}
}

func TestRetriesExhausted(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.failAlwaysWith(status.Error(codes.Unavailable, "down"))
	cfg := fastConfig(srv.addr)
	cfg.MaxRetries = 2
	c := newTestClient(t, cfg)

	_, err := c.Get(testContext(t), []byte("k"))
	if err == nil {
		t.Fatal("Get should fail once retries are exhausted")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("exhausted error should wrap the last status, got %v", err)
	}
	if srv.callCount("Get") != 3 {
		t.Fatalf("attempts = %d, want MaxRetries+1 = 3", srv.callCount("Get"))
	}
}

func TestRotatesPastDeadEndpoint(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	dead := deadAddress(t)
	c := newTestClient(t, fastConfig(dead, srv.addr))

	if _, err := c.Put(testContext(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put via rotation: %v", err)
	}
	if got := c.endpoints.current(); got != srv.addr {
		t.Fatalf("preferred endpoint should move to the live node, got %s", got)
	}
	if _, err := c.Get(testContext(t), []byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if srv.callCount("Get") != 1 {
		t.Fatalf("Get should hit the live node first, calls = %d", srv.callCount("Get"))
	}
}

func TestContextCancelledDuringRetries(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.failAlwaysWith(status.Error(codes.Unavailable, "down"))
	cfg := fastConfig(srv.addr)
	cfg.MaxRetries = 1000
	cfg.RetryBaseBackoff = 50 * time.Millisecond
	cfg.RetryMaxBackoff = 50 * time.Millisecond
	c := newTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Get(ctx, []byte("k"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Get took %v after context expiry", elapsed)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("the last attempt error should still be inspectable, got %v", err)
	}

	already, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := c.Get(already, []byte("k")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestCloseRejectsFurtherCalls(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c, err := New(fastConfig(srv.addr))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.Put(testContext(t), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := c.Get(testContext(t), []byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if _, err := c.Watch(testContext(t), []byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Watch after Close = %v, want ErrClosed", err)
	}
}

func TestAdminRoundTrip(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)

	st, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.ID != 1 || st.State != "leader" || st.Leader != 1 || st.Term != 7 || st.Version != "test" || len(st.Members) != 1 {
		t.Fatalf("unexpected status: %+v", st)
	}

	members, leader, err := c.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if leader != 1 || len(members) != 1 || members[0].ID != 1 || members[0].ClientAddress != srv.addr {
		t.Fatalf("unexpected members: %+v leader=%d", members, leader)
	}

	members, err = c.AddMember(ctx, Member{ID: 2, RaftAddress: "r2", ClientAddress: "c2", Learner: true})
	if err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	if len(members) != 2 || !members[1].Learner || members[1].RaftAddress != "r2" {
		t.Fatalf("unexpected members after add: %+v", members)
	}
	if _, err := c.AddMember(ctx, Member{ID: 2}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate AddMember error = %v, want AlreadyExists", err)
	}

	members, err = c.PromoteMember(ctx, 2)
	if err != nil {
		t.Fatalf("PromoteMember: %v", err)
	}
	if members[1].Learner {
		t.Fatal("member 2 should be a voter after promotion")
	}

	if err := c.TransferLeadership(ctx, 2); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if err := c.TransferLeadership(ctx, 9); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("TransferLeadership(unknown) error = %v, want InvalidArgument", err)
	}

	members, err = c.RemoveMember(ctx, 2)
	if err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("unexpected members after remove: %+v", members)
	}
	if _, err := c.RemoveMember(ctx, 2); status.Code(err) != codes.NotFound {
		t.Fatalf("RemoveMember(missing) error = %v, want NotFound", err)
	}

	if _, err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	idx, err := c.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if idx != 1 {
		t.Fatalf("snapshot index = %d, want 1", idx)
	}
}

func TestBackoffBounds(t *testing.T) {
	t.Parallel()
	c := newTestClient(t, Config{Endpoints: []string{"127.0.0.1:1"}, RetryBaseBackoff: 10 * time.Millisecond, RetryMaxBackoff: 80 * time.Millisecond})
	for attempt := range 10 {
		expected := min(10*time.Millisecond<<attempt, 80*time.Millisecond)
		for range 20 {
			d := c.backoff(attempt)
			if d < expected/2 || d > expected {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v]", attempt, d, expected/2, expected)
			}
		}
	}
}

func TestEndpointRing(t *testing.T) {
	t.Parallel()
	r := newEndpointRing([]string{"a", "b", "c"})
	if r.current() != "a" {
		t.Fatalf("initial current = %s, want a", r.current())
	}
	if r.next("a") != "b" || r.next("b") != "c" || r.next("c") != "a" {
		t.Fatal("next should cycle through the ring")
	}
	r.prefer("d")
	if r.current() != "d" || len(r.all()) != 4 || r.next("d") != "a" {
		t.Fatalf("prefer(unknown) should append and select it: current=%s all=%v", r.current(), r.all())
	}
	r.prefer("b")
	if r.current() != "b" || len(r.all()) != 4 {
		t.Fatalf("prefer(known) should not duplicate: current=%s all=%v", r.current(), r.all())
	}
}
