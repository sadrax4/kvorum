package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// waitForWatcher blocks until the fake server has registered n watchers so
// that writes made afterwards are guaranteed to be observed.
func waitForWatchers(t *testing.T, srv *fakeServer, n int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		got := len(srv.watchers)
		srv.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("server never registered %d watcher(s)", n)
}

func recvEvent(t *testing.T, ch <-chan WatchEvent) WatchEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return ev
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a watch event")
	}
	return WatchEvent{}
}

func expectClosed(t *testing.T, ch <-chan WatchEvent) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("expected the channel to be closed, got event %+v", ev)
		}
	case <-time.After(testTimeout):
		t.Fatal("watch channel was not closed")
	}
}

func TestWatchDeliversAndClosesOnCancel(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()

	ch, err := c.Watch(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	waitForWatchers(t, srv, 1)

	if _, err := c.Put(ctx, []byte("other"), []byte("x")); err != nil {
		t.Fatalf("Put(other): %v", err)
	}
	if _, err := c.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ev := recvEvent(t, ch)
	if ev.Err != nil || ev.Type != EventPut || !bytes.Equal(ev.KV.Key, []byte("k")) || !bytes.Equal(ev.KV.Value, []byte("v1")) {
		t.Fatalf("unexpected first event: %+v", ev)
	}
	if ev.Header.MemberID != 1 || ev.Header.Revision == 0 {
		t.Fatalf("event should carry the stream header: %+v", ev.Header)
	}

	if _, err := c.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ev = recvEvent(t, ch)
	if ev.Type != EventDelete || ev.PrevKV == nil || !bytes.Equal(ev.PrevKV.Value, []byte("v1")) {
		t.Fatalf("unexpected delete event: %+v", ev)
	}

	cancel()
	final := recvEvent(t, ch)
	if !errors.Is(final.Err, context.Canceled) {
		t.Fatalf("final event error = %v, want context.Canceled", final.Err)
	}
	expectClosed(t, ch)
}

func TestWatchPrefixAndRange(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)

	prefixCh, err := c.Watch(ctx, []byte("p/"), WithPrefix())
	if err != nil {
		t.Fatalf("Watch(prefix): %v", err)
	}
	rangeCh, err := c.Watch(ctx, []byte("a"), WithRange([]byte("c")))
	if err != nil {
		t.Fatalf("Watch(range): %v", err)
	}
	waitForWatchers(t, srv, 2)

	for _, k := range []string{"zzz", "p/1", "b", "c", "p/2"} {
		if _, err := c.Put(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	if ev := recvEvent(t, prefixCh); string(ev.KV.Key) != "p/1" {
		t.Fatalf("prefix watch first key = %q, want p/1", ev.KV.Key)
	}
	if ev := recvEvent(t, prefixCh); string(ev.KV.Key) != "p/2" {
		t.Fatalf("prefix watch second key = %q, want p/2", ev.KV.Key)
	}
	if ev := recvEvent(t, rangeCh); string(ev.KV.Key) != "b" {
		t.Fatalf("range watch key = %q, want b", ev.KV.Key)
	}
	select {
	case ev := <-rangeCh:
		t.Fatalf("range watch should not see %q", ev.KV.Key)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchTerminalError(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.mu.Lock()
	srv.watchErr = status.Error(codes.InvalidArgument, "bad range")
	srv.mu.Unlock()
	c := newTestClient(t, fastConfig(srv.addr))

	ch, err := c.Watch(testContext(t), []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	ev := recvEvent(t, ch)
	if status.Code(ev.Err) != codes.InvalidArgument {
		t.Fatalf("terminal event error = %v, want InvalidArgument", ev.Err)
	}
	expectClosed(t, ch)
	if srv.callCount("Watch") != 1 {
		t.Fatalf("a non-retryable watch error must not be retried, attempts = %d", srv.callCount("Watch"))
	}
}

func TestWatchGivesUpAfterRetries(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.failAlwaysWith(status.Error(codes.Unavailable, "down"))
	cfg := fastConfig(srv.addr)
	cfg.MaxRetries = 2
	c := newTestClient(t, cfg)

	ch, err := c.Watch(testContext(t), []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	ev := recvEvent(t, ch)
	if status.Code(ev.Err) != codes.Unavailable {
		t.Fatalf("terminal event error = %v, want Unavailable", ev.Err)
	}
	expectClosed(t, ch)
	if srv.callCount("Watch") != 3 {
		t.Fatalf("watch attempts = %d, want MaxRetries+1 = 3", srv.callCount("Watch"))
	}
}

func TestWatchFollowsLeaderHint(t *testing.T) {
	t.Parallel()
	follower := startFakeServer(t, 1)
	leader := startFakeServer(t, 2)
	follower.setLeader(leader.addr)
	c := newTestClient(t, fastConfig(follower.addr))
	ctx := testContext(t)

	ch, err := c.Watch(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	waitForWatchers(t, leader, 1)
	if _, err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ev := recvEvent(t, ch)
	if ev.Err != nil || ev.Header.MemberID != 2 {
		t.Fatalf("event should come from the leader: %+v", ev)
	}
}

func TestWatchReconnectsAfterEOF(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	srv.mu.Lock()
	srv.watchEOF = true
	srv.mu.Unlock()
	c := newTestClient(t, fastConfig(srv.addr))
	ctx := testContext(t)

	ch, err := c.Watch(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	waitForWatchers(t, srv, 1)
	if _, err := c.Put(ctx, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ev := recvEvent(t, ch); ev.Err != nil || string(ev.KV.Value) != "v1" {
		t.Fatalf("unexpected first event: %+v", ev)
	}
	// The server ended the stream after the first event; the client must
	// re-register and keep delivering.
	deadline := time.Now().Add(testTimeout)
	for srv.callCount("Watch") < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	waitForWatchers(t, srv, 1)
	if _, err := c.Put(ctx, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ev := recvEvent(t, ch); ev.Err != nil || string(ev.KV.Value) != "v2" {
		t.Fatalf("unexpected event after reconnect: %+v", ev)
	}
}

func TestCloseEndsWatch(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c, err := New(fastConfig(srv.addr))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ch, err := c.Watch(testContext(t), []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	waitForWatchers(t, srv, 1)

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Close did not return while a watch was active")
	}
	ev := recvEvent(t, ch)
	if !errors.Is(ev.Err, ErrClosed) {
		t.Fatalf("final event error = %v, want ErrClosed", ev.Err)
	}
	expectClosed(t, ch)
}

func TestWatchTerminalEventDroppedWhenNotDrained(t *testing.T) {
	t.Parallel()
	srv := startFakeServer(t, 1)
	c := newTestClient(t, fastConfig(srv.addr))
	ctx, cancel := context.WithCancel(testContext(t))

	ch, err := c.Watch(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	waitForWatchers(t, srv, 1)
	// Fill the channel beyond its buffer without reading from it.
	for range watchBuffer + 5 {
		if _, err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	cancel()
	// Drain: every buffered event is a PUT and the channel ends up closed
	// even though the terminal event could not be delivered.
	deadline := time.After(testTimeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.Err != nil && !errors.Is(ev.Err, context.Canceled) {
				t.Fatalf("unexpected error event: %v", ev.Err)
			}
		case <-deadline:
			t.Fatal("channel never closed")
		}
	}
}
