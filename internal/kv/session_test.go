package kv

import (
	"errors"
	"testing"
)

func TestDedupDuplicateReturnsRecordedResult(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("k"), nil, 8)
	cmd := withPrevKV(withSession(put("k", "v1"), 7, 1))

	first := mustApply(t, s, 1, cmd)
	if first.Duplicate {
		t.Fatal("first execution flagged as duplicate")
	}
	second := mustApply(t, s, 2, cmd)
	if !second.Duplicate {
		t.Fatal("retry not flagged as duplicate")
	}
	assertKV(t, "duplicate KV", second.KV, kv("k", "v1", 1, 1, 1))
	assertNoKV(t, "duplicate PrevKV", second.PrevKV)
	assertGet(t, s, kv("k", "v1", 1, 1, 1))
	if s.Revision() != 2 {
		t.Fatalf("Revision() = %d, want 2", s.Revision())
	}
	if s.Sessions() != 1 {
		t.Fatalf("Sessions() = %d, want 1", s.Sessions())
	}
	recvEvent(t, w)
	assertNoEvent(t, w)
}

func TestDedupStaleSequence(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, withSession(put("k", "v1"), 7, 5))

	res := s.Apply(2, withSession(put("k", "v2"), 7, 4))
	if !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("Err = %v, want ErrStaleSequence", res.Err)
	}
	if res.KV != nil || res.Duplicate {
		t.Fatalf("stale command produced a result: %+v", res)
	}
	assertGet(t, s, kv("k", "v1", 1, 1, 1))
	if s.Revision() != 2 {
		t.Fatalf("Revision() = %d, want 2", s.Revision())
	}

	res = mustApply(t, s, 3, withSession(put("k", "v3"), 7, 6))
	assertKV(t, "KV", res.KV, kv("k", "v3", 2, 1, 3))
}

func TestDedupSequencesNeedNotBeContiguous(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, withSession(put("k", "v1"), 1, 1))
	res := mustApply(t, s, 2, withSession(put("k", "v2"), 1, 10))
	if res.Duplicate || res.KV.Version != 2 {
		t.Fatalf("sequence jump not applied: %+v", res)
	}
	res = mustApply(t, s, 3, withSession(put("k", "v2"), 1, 10))
	if !res.Duplicate {
		t.Fatal("retry of the jumped sequence not recognized")
	}
	if res := s.Apply(4, withSession(put("k", "v1"), 1, 1)); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("old sequence: Err = %v, want ErrStaleSequence", res.Err)
	}
}

func TestDedupClientsAreIndependent(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, withSession(put("k", "a"), 1, 1))
	res := mustApply(t, s, 2, withSession(put("k", "b"), 2, 1))
	if res.Duplicate {
		t.Fatal("same sequence from another client treated as duplicate")
	}
	assertGet(t, s, kv("k", "b", 2, 1, 2))
	if s.Sessions() != 2 {
		t.Fatalf("Sessions() = %d, want 2", s.Sessions())
	}
}

func TestDedupDisabledForZeroClientID(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	cmd := Command{Op: OpPut, Key: []byte("k"), Value: []byte("v"), Sequence: 1}
	mustApply(t, s, 1, cmd)
	res := mustApply(t, s, 2, cmd)
	if res.Duplicate || res.KV.Version != 2 {
		t.Fatalf("second apply without a client id = %+v, want version 2", res)
	}
	if s.Sessions() != 0 {
		t.Fatalf("Sessions() = %d, want 0", s.Sessions())
	}
}

func TestDedupRecordedResultIsIsolated(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	cmd := withPrevKV(withSession(put("k", "v2"), 3, 2))
	mustApply(t, s, 1, put("k", "v1"))
	first := mustApply(t, s, 2, cmd)
	first.KV.Value[0], first.PrevKV.Value[0] = 'X', 'X'

	second := mustApply(t, s, 3, cmd)
	second.KV.Value[0], second.PrevKV.Value[0] = 'Y', 'Y'

	third := mustApply(t, s, 4, cmd)
	assertKV(t, "third KV", third.KV, kv("k", "v2", 2, 1, 2))
	assertKV(t, "third PrevKV", third.PrevKV, kv("k", "v1", 1, 1, 1))
}

func TestDedupRecordsFailedCompareAndSwap(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, put("k", "v1"))
	cmd := withSession(cas("k", "v2", 9), 3, 1)
	first := mustApply(t, s, 2, cmd)
	if first.Succeeded {
		t.Fatal("CAS with a wrong version succeeded")
	}
	mustApply(t, s, 3, put("k", "v3"))

	retry := mustApply(t, s, 4, cmd)
	if !retry.Duplicate || retry.Succeeded {
		t.Fatalf("retry = %+v, want a duplicate failure", retry)
	}
	assertKV(t, "retry KV", retry.KV, kv("k", "v1", 1, 1, 1))
	assertGet(t, s, kv("k", "v3", 2, 1, 3))
}

func TestDedupRecordsDeleteAndCreate(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, put("k", "v1"))
	cmd := withPrevKV(withSession(del("k"), 5, 1))
	mustApply(t, s, 2, cmd)
	mustApply(t, s, 3, put("k", "v2"))

	retry := mustApply(t, s, 4, cmd)
	if !retry.Duplicate || !retry.Deleted {
		t.Fatalf("retry = %+v, want duplicate delete", retry)
	}
	assertKV(t, "retry PrevKV", retry.PrevKV, kv("k", "v1", 1, 1, 1))
	assertGet(t, s, kv("k", "v2", 1, 3, 3))

	create := withSession(cas("n", "v", 0), 5, 2)
	mustApply(t, s, 5, create)
	retry = mustApply(t, s, 6, create)
	if !retry.Duplicate || !retry.Succeeded {
		t.Fatalf("create retry = %+v, want duplicate success", retry)
	}
	assertGet(t, s, kv("n", "v", 1, 5, 5))
}

func TestDedupInvalidCommandNotRecorded(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	if res := s.Apply(1, Command{Op: 77, Key: []byte("k"), ClientID: 1, Sequence: 1}); res.Err == nil {
		t.Fatal("invalid command accepted")
	}
	if s.Sessions() != 0 {
		t.Fatal("invalid command created a session")
	}
	res := mustApply(t, s, 2, withSession(put("k", "v"), 1, 1))
	if res.Duplicate {
		t.Fatal("valid command after an invalid one with the same sequence treated as duplicate")
	}
}

func TestSessionEvictionIsLeastRecentlyUsedByRevision(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{MaxSessions: 2})
	mustApply(t, s, 1, withSession(put("k1", "v"), 1, 1))
	mustApply(t, s, 2, withSession(put("k2", "v"), 2, 1))
	if res := mustApply(t, s, 3, withSession(put("k1", "v"), 1, 1)); !res.Duplicate {
		t.Fatal("client 1 retry not recognized")
	}
	// Client 2 was last used at revision 2, client 1 at revision 3.
	mustApply(t, s, 4, withSession(put("k3", "v"), 3, 1))
	if s.Sessions() != 2 {
		t.Fatalf("Sessions() = %d, want 2", s.Sessions())
	}
	res := mustApply(t, s, 5, withSession(put("k2", "v"), 2, 1))
	if res.Duplicate || res.KV.Version != 2 {
		t.Fatalf("evicted client 2 retry = %+v, want a fresh execution", res)
	}
	// Client 1 (revision 3) is now the oldest and was evicted by client 2.
	if res := mustApply(t, s, 6, withSession(put("k1", "v"), 1, 1)); res.Duplicate {
		t.Fatal("client 1 should have been evicted")
	}
	if res := mustApply(t, s, 7, withSession(put("k2", "v"), 2, 1)); !res.Duplicate {
		t.Fatal("client 2 should still be present")
	}
	if s.Sessions() != 2 {
		t.Fatalf("Sessions() = %d, want 2", s.Sessions())
	}
}

func TestSessionStaleLookupCountsAsUse(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{MaxSessions: 2})
	mustApply(t, s, 1, withSession(put("k1", "v"), 1, 5))
	mustApply(t, s, 2, withSession(put("k2", "v"), 2, 1))
	if res := s.Apply(3, withSession(put("k1", "v"), 1, 4)); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("Err = %v, want ErrStaleSequence", res.Err)
	}
	mustApply(t, s, 4, withSession(put("k3", "v"), 3, 1))
	if res := mustApply(t, s, 5, withSession(put("k1", "v"), 1, 5)); !res.Duplicate {
		t.Fatal("client 1 was evicted although it was used more recently than client 2")
	}
}

func TestDefaultMaxSessions(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, -5} {
		s := NewStore(Options{MaxSessions: limit})
		if s.maxSessions != DefaultMaxSessions || s.sessions.limit != DefaultMaxSessions {
			t.Fatalf("MaxSessions %d resolved to %d, want %d", limit, s.maxSessions, DefaultMaxSessions)
		}
	}
}
