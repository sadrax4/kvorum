package kv

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func put(key, value string) Command {
	return Command{Op: OpPut, Key: []byte(key), Value: []byte(value)}
}

func del(key string) Command {
	return Command{Op: OpDelete, Key: []byte(key)}
}

func cas(key, value string, expected uint64) Command {
	return Command{Op: OpCompareAndSwap, Key: []byte(key), Value: []byte(value), ExpectedVersion: expected}
}

func withSession(c Command, clientID, sequence uint64) Command {
	c.ClientID = clientID
	c.Sequence = sequence
	return c
}

func withPrevKV(c Command) Command {
	c.PrevKV = true
	return c
}

func mustApply(t *testing.T, s *Store, revision uint64, cmd Command) Result {
	t.Helper()
	res := s.Apply(revision, cmd)
	if res.Err != nil {
		t.Fatalf("Apply(%d, %s %q): %v", revision, cmd.Op, cmd.Key, res.Err)
	}
	return res
}

func kv(key, value string, version, createRevision, modRevision uint64) KeyValue {
	var v []byte
	if value != "" {
		v = []byte(value)
	}
	return KeyValue{Key: []byte(key), Value: v, Version: version, CreateRevision: createRevision, ModRevision: modRevision}
}

func kvEqual(a, b KeyValue) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) &&
		a.Version == b.Version && a.CreateRevision == b.CreateRevision && a.ModRevision == b.ModRevision
}

func assertKV(t *testing.T, what string, got *KeyValue, want KeyValue) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s is nil, want %+v", what, want)
	}
	if !kvEqual(*got, want) {
		t.Fatalf("%s = %+v, want %+v", what, *got, want)
	}
}

func assertNoKV(t *testing.T, what string, got *KeyValue) {
	t.Helper()
	if got != nil {
		t.Fatalf("%s = %+v, want nil", what, *got)
	}
}

func assertGet(t *testing.T, s *Store, want KeyValue) {
	t.Helper()
	got, ok := s.Get(want.Key)
	if !ok {
		t.Fatalf("Get(%q) found nothing, want %+v", want.Key, want)
	}
	if !kvEqual(got, want) {
		t.Fatalf("Get(%q) = %+v, want %+v", want.Key, got, want)
	}
}

func assertMissing(t *testing.T, s *Store, key string) {
	t.Helper()
	if got, ok := s.Get([]byte(key)); ok {
		t.Fatalf("Get(%q) = %+v, want missing", key, got)
	}
}

// recvEvent returns the next event or fails if none arrives promptly. Events
// are dispatched synchronously by Apply, so a short timeout is generous.
func recvEvent(t *testing.T, w *Watcher) Event {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatalf("watcher channel closed: %v", w.Err())
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered")
	}
	return Event{}
}

// assertNoEvent checks that nothing is pending on the watcher. Dispatch is
// synchronous, so a non-blocking receive is sufficient.
func assertNoEvent(t *testing.T, w *Watcher) {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatalf("watcher channel closed unexpectedly: %v", w.Err())
		}
		t.Fatalf("unexpected event %s %q at revision %d", ev.Type, ev.KV.Key, ev.Revision)
	default:
	}
}

// assertClosed checks that the watcher's channel is closed with no events
// left and that Err reports want.
func assertClosed(t *testing.T, w *Watcher, want error) {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if ok {
			t.Fatalf("channel still delivers %s %q at revision %d", ev.Type, ev.KV.Key, ev.Revision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
	if err := w.Err(); !errors.Is(err, want) {
		t.Fatalf("Err() = %v, want %v", err, want)
	}
}
