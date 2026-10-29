package kv

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestNewStoreIsEmpty(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	if s.Revision() != 0 || s.Len() != 0 || s.Sessions() != 0 || s.Watchers() != 0 {
		t.Fatalf("new store is not empty: revision=%d len=%d sessions=%d watchers=%d",
			s.Revision(), s.Len(), s.Sessions(), s.Watchers())
	}
	assertMissing(t, s, "anything")
	if kvs, more := s.Scan(nil, nil, 0); len(kvs) != 0 || more {
		t.Fatalf("Scan on empty store = %v, %t", kvs, more)
	}
}

func TestApplyPut(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})

	res := mustApply(t, s, 1, put("k", "v1"))
	assertKV(t, "KV", res.KV, kv("k", "v1", 1, 1, 1))
	assertNoKV(t, "PrevKV", res.PrevKV)
	if res.Succeeded || res.Deleted || res.Duplicate {
		t.Fatalf("flags = %+v, want all false", res)
	}

	res = mustApply(t, s, 5, withPrevKV(put("k", "v2")))
	assertKV(t, "KV", res.KV, kv("k", "v2", 2, 1, 5))
	assertKV(t, "PrevKV", res.PrevKV, kv("k", "v1", 1, 1, 1))

	res = mustApply(t, s, 6, put("k", "v3"))
	assertKV(t, "KV", res.KV, kv("k", "v3", 3, 1, 6))
	assertNoKV(t, "PrevKV without the flag", res.PrevKV)

	assertGet(t, s, kv("k", "v3", 3, 1, 6))
	if s.Len() != 1 || s.Revision() != 6 {
		t.Fatalf("len=%d revision=%d, want 1 and 6", s.Len(), s.Revision())
	}
}

func TestApplyPutPrevKVOnlyWhenKeyExisted(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	res := mustApply(t, s, 1, withPrevKV(put("new", "v")))
	assertNoKV(t, "PrevKV on creation", res.PrevKV)
	assertKV(t, "KV", res.KV, kv("new", "v", 1, 1, 1))
}

func TestApplyPutEmptyValue(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, put("k", ""))
	got, ok := s.Get([]byte("k"))
	if !ok || len(got.Value) != 0 || got.Version != 1 {
		t.Fatalf("Get after empty put = %+v, %t", got, ok)
	}
}

func TestApplyDelete(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})

	res := mustApply(t, s, 1, withPrevKV(del("missing")))
	if res.Deleted || res.KV != nil || res.PrevKV != nil {
		t.Fatalf("deleting a missing key = %+v, want nothing", res)
	}

	mustApply(t, s, 2, put("k", "v1"))
	mustApply(t, s, 3, put("k", "v2"))
	res = mustApply(t, s, 4, withPrevKV(del("k")))
	if !res.Deleted {
		t.Fatal("Deleted = false for an existing key")
	}
	assertNoKV(t, "KV", res.KV)
	assertKV(t, "PrevKV", res.PrevKV, kv("k", "v2", 2, 2, 3))
	assertMissing(t, s, "k")
	if s.Len() != 0 {
		t.Fatalf("Len() = %d after delete, want 0", s.Len())
	}

	res = mustApply(t, s, 5, del("k"))
	if res.Deleted {
		t.Fatal("Deleted = true for an already deleted key")
	}

	// Recreating the key starts a new version history.
	res = mustApply(t, s, 6, put("k", "v3"))
	assertKV(t, "KV after recreation", res.KV, kv("k", "v3", 1, 6, 6))

	res = mustApply(t, s, 7, del("k"))
	if !res.Deleted || res.PrevKV != nil {
		t.Fatalf("delete without PrevKV = %+v", res)
	}
}

func TestApplyCompareAndSwap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		setup         []Command
		cmd           Command
		wantSucceeded bool
		wantKV        *KeyValue
		wantPrevKV    *KeyValue
		wantStored    *KeyValue
	}{
		{
			name:          "create if absent",
			cmd:           cas("k", "v1", 0),
			wantSucceeded: true,
			wantKV:        ptr(kv("k", "v1", 1, 1, 1)),
			wantStored:    ptr(kv("k", "v1", 1, 1, 1)),
		},
		{
			name:       "expected zero but key exists",
			setup:      []Command{put("k", "v1")},
			cmd:        cas("k", "v2", 0),
			wantKV:     ptr(kv("k", "v1", 1, 1, 1)),
			wantStored: ptr(kv("k", "v1", 1, 1, 1)),
		},
		{
			name:       "wrong version",
			setup:      []Command{put("k", "v1")},
			cmd:        cas("k", "v2", 7),
			wantKV:     ptr(kv("k", "v1", 1, 1, 1)),
			wantStored: ptr(kv("k", "v1", 1, 1, 1)),
		},
		{
			name:          "matching version",
			setup:         []Command{put("k", "v1"), put("k", "v2")},
			cmd:           withPrevKV(cas("k", "v3", 2)),
			wantSucceeded: true,
			wantKV:        ptr(kv("k", "v3", 3, 1, 3)),
			wantPrevKV:    ptr(kv("k", "v2", 2, 1, 2)),
			wantStored:    ptr(kv("k", "v3", 3, 1, 3)),
		},
		{
			name: "absent key with non-zero expected version",
			cmd:  cas("k", "v1", 3),
		},
		{
			name:       "prev kv requested on failure is not returned",
			setup:      []Command{put("k", "v1")},
			cmd:        withPrevKV(cas("k", "v2", 5)),
			wantKV:     ptr(kv("k", "v1", 1, 1, 1)),
			wantStored: ptr(kv("k", "v1", 1, 1, 1)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewStore(Options{})
			rev := uint64(0)
			for _, c := range tc.setup {
				rev++
				mustApply(t, s, rev, c)
			}
			rev++
			res := mustApply(t, s, rev, tc.cmd)
			if res.Succeeded != tc.wantSucceeded {
				t.Fatalf("Succeeded = %t, want %t", res.Succeeded, tc.wantSucceeded)
			}
			if tc.wantKV == nil {
				assertNoKV(t, "KV", res.KV)
			} else {
				assertKV(t, "KV", res.KV, *tc.wantKV)
			}
			if tc.wantPrevKV == nil {
				assertNoKV(t, "PrevKV", res.PrevKV)
			} else {
				assertKV(t, "PrevKV", res.PrevKV, *tc.wantPrevKV)
			}
			if tc.wantStored == nil {
				assertMissing(t, s, "k")
			} else {
				assertGet(t, s, *tc.wantStored)
			}
			if s.Revision() != rev {
				t.Fatalf("Revision() = %d, want %d", s.Revision(), rev)
			}
		})
	}
}

func ptr(kv KeyValue) *KeyValue { return &kv }

func TestApplyInvalidCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cmd  Command
		want error
	}{
		{name: "zero op", cmd: Command{Key: []byte("k"), ClientID: 1, Sequence: 1}, want: ErrUnknownOp},
		{name: "unknown op", cmd: Command{Op: 42, Key: []byte("k"), ClientID: 1, Sequence: 1}, want: ErrUnknownOp},
		{name: "empty key", cmd: Command{Op: OpPut, Value: []byte("v"), ClientID: 1, Sequence: 1}, want: ErrEmptyKey},
		{name: "oversized key", cmd: Command{Op: OpPut, Key: make([]byte, MaxKeySize+1), ClientID: 1, Sequence: 1}, want: ErrKeyTooLarge},
		{name: "oversized value", cmd: Command{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueSize+1), ClientID: 1, Sequence: 1}, want: ErrValueTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := NewStore(Options{})
			mustApply(t, s, 1, put("k", "v"))
			res := s.Apply(2, tc.cmd)
			if !errors.Is(res.Err, tc.want) || !errors.Is(res.Err, ErrInvalidCommand) {
				t.Fatalf("Err = %v, want %v wrapping ErrInvalidCommand", res.Err, tc.want)
			}
			if res.KV != nil || res.PrevKV != nil || res.Succeeded || res.Deleted || res.Duplicate {
				t.Fatalf("invalid command produced a non-empty result: %+v", res)
			}
			assertGet(t, s, kv("k", "v", 1, 1, 1))
			if s.Len() != 1 {
				t.Fatalf("Len() = %d, want 1", s.Len())
			}
			if s.Revision() != 2 {
				t.Fatalf("Revision() = %d, want 2: an invalid command still consumes its revision", s.Revision())
			}
			if s.Sessions() != 0 {
				t.Fatal("an invalid command must not be recorded in the session table")
			}
		})
	}
}

func TestApplyRevisionMustIncrease(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})

	if res := s.Apply(0, put("k", "v")); !errors.Is(res.Err, ErrRevisionNotMonotonic) {
		t.Fatalf("Apply(0) on a new store: Err = %v, want ErrRevisionNotMonotonic", res.Err)
	}
	mustApply(t, s, 10, put("k", "v1"))
	for _, rev := range []uint64{10, 3, 0} {
		res := s.Apply(rev, put("k", "v2"))
		if !errors.Is(res.Err, ErrRevisionNotMonotonic) {
			t.Fatalf("Apply(%d) after 10: Err = %v, want ErrRevisionNotMonotonic", rev, res.Err)
		}
		if res.KV != nil {
			t.Fatal("rejected command returned a KV")
		}
	}
	assertGet(t, s, kv("k", "v1", 1, 10, 10))
	if s.Revision() != 10 {
		t.Fatalf("Revision() = %d after rejected applies, want 10", s.Revision())
	}
	mustApply(t, s, 11, put("k", "v2"))
	assertGet(t, s, kv("k", "v2", 2, 10, 11))
	if !strings.Contains(s.Apply(11, put("k", "v3")).Err.Error(), "got 11 after 11") {
		t.Fatal("error message should name both revisions")
	}
}

func TestApplyCopiesInputSlices(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	key := []byte("key")
	value := []byte("value")
	mustApply(t, s, 1, Command{Op: OpPut, Key: key, Value: value})
	key[0], value[0] = 'X', 'X'
	assertGet(t, s, kv("key", "value", 1, 1, 1))

	key, value = []byte("key"), []byte("other")
	mustApply(t, s, 2, Command{Op: OpPut, Key: key, Value: value})
	value[0] = 'X'
	assertGet(t, s, kv("key", "other", 2, 1, 2))
}

func TestOutputsDoNotAliasStorage(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	want := kv("key", "value", 1, 1, 1)

	res := mustApply(t, s, 1, put("key", "value"))
	res.KV.Key[0], res.KV.Value[0] = 'X', 'X'
	assertGet(t, s, want)

	got, _ := s.Get([]byte("key"))
	got.Key[0], got.Value[0] = 'X', 'X'
	assertGet(t, s, want)

	kvs, _ := s.Scan(nil, nil, 0)
	kvs[0].Key[0], kvs[0].Value[0] = 'X', 'X'
	assertGet(t, s, want)

	res = mustApply(t, s, 2, withPrevKV(put("key", "value2")))
	res.PrevKV.Value[0] = 'X'
	res = mustApply(t, s, 3, withPrevKV(cas("key", "value3", 99)))
	res.KV.Value[0] = 'X'
	assertGet(t, s, kv("key", "value2", 2, 1, 2))
}

func TestScan(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		mustApply(t, s, uint64(i+1), put(k, "v"+k))
	}
	tests := []struct {
		name       string
		start, end string
		limit      int
		wantKeys   []string
		wantMore   bool
	}{
		{name: "everything", wantKeys: keys},
		{name: "negative limit is unlimited", limit: -1, wantKeys: keys},
		{name: "limit below size", limit: 2, wantKeys: []string{"a", "b"}, wantMore: true},
		{name: "limit equal to size", limit: 5, wantKeys: keys},
		{name: "limit above size", limit: 6, wantKeys: keys},
		{name: "closed range", start: "b", end: "d", wantKeys: []string{"b", "c"}},
		{name: "closed range with limit", start: "b", end: "e", limit: 2, wantKeys: []string{"b", "c"}, wantMore: true},
		{name: "limit hits exactly the range end", start: "b", end: "d", limit: 2, wantKeys: []string{"b", "c"}},
		{name: "open end", start: "c", wantKeys: []string{"c", "d", "e"}},
		{name: "open start", end: "c", wantKeys: []string{"a", "b"}},
		{name: "start between keys", start: "bb", end: "dd", wantKeys: []string{"c", "d"}},
		{name: "inverted range", start: "d", end: "b"},
		{name: "beyond the last key", start: "z"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kvs, more := s.Scan([]byte(tc.start), []byte(tc.end), tc.limit)
			if more != tc.wantMore {
				t.Fatalf("more = %t, want %t", more, tc.wantMore)
			}
			if len(kvs) != len(tc.wantKeys) {
				t.Fatalf("got %d keys, want %d", len(kvs), len(tc.wantKeys))
			}
			for i, k := range tc.wantKeys {
				if !kvEqual(kvs[i], kv(k, "v"+k, 1, uint64(k[0]-'a'+1), uint64(k[0]-'a'+1))) {
					t.Fatalf("kvs[%d] = %+v, want key %q", i, kvs[i], k)
				}
			}
		})
	}
}

func TestScanLargeLimitDoesNotOverAllocate(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, put("a", "1"))
	kvs, more := s.Scan(nil, nil, 1<<30)
	if len(kvs) != 1 || more {
		t.Fatalf("Scan = %d keys, more=%t", len(kvs), more)
	}
	if cap(kvs) > scanPreallocLimit {
		t.Fatalf("Scan reserved capacity %d for a huge limit", cap(kvs))
	}
}

func TestConcurrentReadersDuringApply(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s.Get([]byte("key-7"))
				s.Scan([]byte("key-1"), []byte("key-4"), 10)
				s.Revision()
				s.Len()
				if i == 0 {
					if _, err := s.Snapshot(); err != nil {
						t.Errorf("Snapshot: %v", err)
						return
					}
				}
			}
		}()
	}
	for rev := uint64(1); rev <= 2000; rev++ {
		key := fmt.Sprintf("key-%d", rev%50)
		if rev%7 == 0 {
			mustApply(t, s, rev, del(key))
		} else {
			mustApply(t, s, rev, withSession(put(key, "v"), rev%5+1, rev))
		}
	}
	close(stop)
	wg.Wait()
	if s.Revision() != 2000 {
		t.Fatalf("Revision() = %d, want 2000", s.Revision())
	}
}
