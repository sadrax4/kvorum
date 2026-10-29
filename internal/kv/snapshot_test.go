package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"testing"
)

// populate applies a workload that exercises every kind of metadata: multiple
// versions, empty values, deletes, recreations, and sessions holding every
// shape of result.
func populate(t *testing.T, s *Store) uint64 {
	t.Helper()
	cmds := []Command{
		put("a", "a1"),
		put("b", "b1"),
		put("a", "a2"),
		put("c", ""),
		del("b"),
		withPrevKV(withSession(put("d", "d1"), 9, 1)),
		withSession(cas("d", "d2", 99), 9, 2),
		withPrevKV(withSession(del("missing"), 10, 1)),
		withPrevKV(withSession(del("a"), 11, 1)),
		withSession(cas("e", "e1", 0), 12, 3),
		put("a", "a3"),
		withPrevKV(withSession(put("d", "d3"), 13, 1)),
	}
	for i, c := range cmds {
		mustApply(t, s, uint64(i+1), c)
	}
	return uint64(len(cmds))
}

func assertSameState(t *testing.T, a, b *Store) {
	t.Helper()
	if a.Revision() != b.Revision() || a.Len() != b.Len() || a.Sessions() != b.Sessions() {
		t.Fatalf("state differs: revision %d/%d len %d/%d sessions %d/%d",
			a.Revision(), b.Revision(), a.Len(), b.Len(), a.Sessions(), b.Sessions())
	}
	ka, _ := a.Scan(nil, nil, 0)
	kb, _ := b.Scan(nil, nil, 0)
	for i := range ka {
		if !kvEqual(ka[i], kb[i]) {
			t.Fatalf("key %d differs: %+v vs %+v", i, ka[i], kb[i])
		}
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	rev := populate(t, src)
	data, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.HasPrefix(data, []byte(snapshotMagic)) {
		t.Fatal("snapshot does not start with the magic string")
	}

	dst := NewStore(Options{})
	if err := dst.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	assertSameState(t, src, dst)
	if dst.Revision() != rev || dst.Len() != 4 || dst.Sessions() != 5 {
		t.Fatalf("restored revision=%d len=%d sessions=%d", dst.Revision(), dst.Len(), dst.Sessions())
	}
	assertGet(t, dst, kv("a", "a3", 1, 11, 11))
	assertGet(t, dst, kv("c", "", 1, 4, 4))
	assertGet(t, dst, kv("d", "d3", 2, 6, 12))
	assertGet(t, dst, kv("e", "e1", 1, 10, 10))
	assertMissing(t, dst, "b")

	// Every recorded result survives, including PrevKV, failures, and deletes.
	retries := []Command{
		withSession(cas("d", "d2", 99), 9, 2),
		withPrevKV(withSession(del("missing"), 10, 1)),
		withPrevKV(withSession(del("a"), 11, 1)),
		withSession(cas("e", "e1", 0), 12, 3),
		withPrevKV(withSession(put("d", "d3"), 13, 1)),
	}
	for i, c := range retries {
		rev++
		want := src.Apply(rev, c)
		got := dst.Apply(rev, c)
		if !want.Duplicate || !got.Duplicate {
			t.Fatalf("retry %d not recognized as duplicate: src=%t dst=%t", i, want.Duplicate, got.Duplicate)
		}
		if want.Succeeded != got.Succeeded || want.Deleted != got.Deleted || (want.KV == nil) != (got.KV == nil) || (want.PrevKV == nil) != (got.PrevKV == nil) {
			t.Fatalf("retry %d differs after restore: %+v vs %+v", i, want, got)
		}
		if want.KV != nil && !kvEqual(*want.KV, *got.KV) {
			t.Fatalf("retry %d KV differs: %+v vs %+v", i, *want.KV, *got.KV)
		}
		if want.PrevKV != nil && !kvEqual(*want.PrevKV, *got.PrevKV) {
			t.Fatalf("retry %d PrevKV differs: %+v vs %+v", i, *want.PrevKV, *got.PrevKV)
		}
	}
	// A stale sequence is still stale after restore.
	if res := dst.Apply(rev+1, withSession(put("d", "x"), 9, 1)); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("Err = %v, want ErrStaleSequence", res.Err)
	}
	// Applying continues from the restored revision.
	if res := dst.Apply(rev+1, put("z", "z")); !errors.Is(res.Err, ErrRevisionNotMonotonic) {
		t.Fatalf("reused revision after restore: Err = %v", res.Err)
	}
	mustApply(t, dst, rev+2, put("z", "z"))
}

func TestSnapshotIsDeterministic(t *testing.T) {
	t.Parallel()
	a, b := NewStore(Options{}), NewStore(Options{})
	populate(t, a)
	populate(t, b)
	da, _ := a.Snapshot()
	db, _ := b.Snapshot()
	if !bytes.Equal(da, db) {
		t.Fatal("two stores with identical history produced different snapshots")
	}
	again, _ := a.Snapshot()
	if !bytes.Equal(da, again) {
		t.Fatal("snapshotting twice produced different bytes")
	}

	c := NewStore(Options{MaxSessions: 7})
	if err := c.Restore(da); err != nil {
		t.Fatal(err)
	}
	dc, _ := c.Snapshot()
	if !bytes.Equal(da, dc) {
		t.Fatal("snapshot of a restored store differs from the original")
	}
}

func TestSnapshotEmptyStore(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	data, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if want := snapshotHeaderSize + 8 + 8 + 8 + snapshotTrailerSize; len(data) != want {
		t.Fatalf("empty snapshot is %d bytes, want %d", len(data), want)
	}
	dst := NewStore(Options{})
	mustApply(t, dst, 5, put("k", "v"))
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	if dst.Revision() != 0 || dst.Len() != 0 {
		t.Fatalf("restored empty snapshot: revision=%d len=%d", dst.Revision(), dst.Len())
	}
	mustApply(t, dst, 1, put("k", "v"))
}

func TestRestoreReplacesExistingState(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	populate(t, src)
	data, _ := src.Snapshot()

	dst := NewStore(Options{})
	mustApply(t, dst, 1, put("x", "x"))
	mustApply(t, dst, 2, withSession(put("y", "y"), 50, 1))
	mustApply(t, dst, 3, put("a", "stale"))
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	assertSameState(t, src, dst)
	assertMissing(t, dst, "x")
	assertMissing(t, dst, "y")
	if res := mustApply(t, dst, src.Revision()+1, withSession(put("y", "y"), 50, 1)); res.Duplicate {
		t.Fatal("session from before the restore survived")
	}
}

func TestRestoreCancelsWatchers(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	populate(t, src)
	data, _ := src.Snapshot()

	dst := NewStore(Options{})
	mustApply(t, dst, 1, put("k", "v"))
	single := dst.Watch([]byte("k"), nil, 4)
	ranged := dst.Watch([]byte("a"), []byte("z"), 4)
	mustApply(t, dst, 2, put("k", "v2"))
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	if dst.Watchers() != 0 {
		t.Fatalf("Watchers() = %d after restore, want 0", dst.Watchers())
	}
	// Events delivered before the restore remain readable, then the channel
	// closes with the overflow error.
	if ev := recvEvent(t, single); ev.Revision != 2 {
		t.Fatalf("buffered event revision = %d, want 2", ev.Revision)
	}
	assertClosed(t, single, ErrWatcherOverflow)
	recvEvent(t, ranged)
	assertClosed(t, ranged, ErrWatcherOverflow)
	single.Cancel()
	if !errors.Is(single.Err(), ErrWatcherOverflow) {
		t.Fatal("Cancel after an overflow replaced the error")
	}

	fresh := dst.Watch([]byte("a"), nil, 4)
	mustApply(t, dst, src.Revision()+1, put("a", "after"))
	if ev := recvEvent(t, fresh); string(ev.KV.Value) != "after" {
		t.Fatalf("watcher created after restore got %+v", ev)
	}
}

func TestRestoreTruncatesSessionsToLimit(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	for i := uint64(1); i <= 5; i++ {
		mustApply(t, src, i, withSession(put("k", "v"), i, 1))
	}
	data, _ := src.Snapshot()
	dst := NewStore(Options{MaxSessions: 2})
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	if dst.Sessions() != 2 {
		t.Fatalf("Sessions() = %d, want 2", dst.Sessions())
	}
	// The two most recently used clients survive. They are checked first
	// because re-applying an evicted client's command inserts a session and
	// evicts another one.
	rev := uint64(5)
	for _, client := range []uint64{5, 4} {
		rev++
		if res := mustApply(t, dst, rev, withSession(put("k", "v"), client, 1)); !res.Duplicate {
			t.Fatalf("client %d was dropped although it was among the most recent", client)
		}
	}
	for _, client := range []uint64{3, 2, 1} {
		rev++
		if res := mustApply(t, dst, rev, withSession(put("k", "v"), client, 1)); res.Duplicate {
			t.Fatalf("client %d survived although the limit excluded it", client)
		}
	}
	if dst.Sessions() != 2 {
		t.Fatalf("Sessions() = %d after re-applies, want 2", dst.Sessions())
	}
}

func TestRestorePreservesEvictionOrder(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{MaxSessions: 3})
	mustApply(t, src, 1, withSession(put("k", "v"), 1, 1))
	mustApply(t, src, 2, withSession(put("k", "v"), 2, 1))
	mustApply(t, src, 3, withSession(put("k", "v"), 3, 1))
	mustApply(t, src, 4, withSession(put("k", "v"), 1, 1)) // touches client 1
	data, _ := src.Snapshot()

	dst := NewStore(Options{MaxSessions: 3})
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	mustApply(t, dst, 5, withSession(put("k", "v"), 4, 1)) // evicts client 2, the lowest revision
	rev := uint64(5)
	for _, client := range []uint64{3, 1, 4} {
		rev++
		if res := mustApply(t, dst, rev, withSession(put("k", "v"), client, 1)); !res.Duplicate {
			t.Fatalf("client %d was evicted although client 2 had the lowest revision", client)
		}
	}
	rev++
	if res := mustApply(t, dst, rev, withSession(put("k", "v"), 2, 1)); res.Duplicate {
		t.Fatal("client 2 survived although it had the lowest revision")
	}
}

// seal appends the CRC-32C trailer to a snapshot body.
func seal(body []byte) []byte {
	return binary.BigEndian.AppendUint32(body, crc32.Checksum(body, crcTable()))
}

// reseal recomputes the trailer of a snapshot whose body was modified.
func reseal(data []byte) []byte {
	return seal(bytes.Clone(data[:len(data)-snapshotTrailerSize]))
}

type sessionRecord struct {
	clientID, sequence, revision uint64
	result                       Result
}

// buildSnapshot encodes an arbitrary, possibly invalid, snapshot body.
func buildSnapshot(revision uint64, keys []KeyValue, sessions []sessionRecord) []byte {
	buf := append([]byte(snapshotMagic), snapshotFormatVersion)
	buf = binary.BigEndian.AppendUint64(buf, revision)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendKeyRecord(buf, k.Key, k.Value, k.Version, k.CreateRevision, k.ModRevision)
	}
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(sessions)))
	for _, s := range sessions {
		buf = binary.BigEndian.AppendUint64(buf, s.clientID)
		buf = binary.BigEndian.AppendUint64(buf, s.sequence)
		buf = binary.BigEndian.AppendUint64(buf, s.revision)
		buf = appendResult(buf, s.result)
	}
	return seal(buf)
}

func TestBuildSnapshotMatchesStoreEncoding(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	mustApply(t, s, 1, put("a", "1"))
	mustApply(t, s, 2, withPrevKV(withSession(put("a", "2"), 4, 9)))
	want, _ := s.Snapshot()
	got := buildSnapshot(2,
		[]KeyValue{kv("a", "2", 2, 1, 2)},
		[]sessionRecord{{clientID: 4, sequence: 9, revision: 2, result: Result{
			KV:     ptr(kv("a", "2", 2, 1, 2)),
			PrevKV: ptr(kv("a", "1", 1, 1, 1)),
		}}})
	if !bytes.Equal(got, want) {
		t.Fatalf("test encoder diverged from Snapshot:\n got % x\nwant % x", got, want)
	}
}

func TestRestoreRejectsCorruptData(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	populate(t, src)
	valid, _ := src.Snapshot()
	good := []KeyValue{kv("a", "1", 1, 1, 1), kv("b", "2", 2, 1, 3)}
	goodSession := sessionRecord{clientID: 1, sequence: 1, revision: 3, result: Result{KV: ptr(kv("a", "1", 1, 1, 1))}}

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty", data: nil, want: ErrCorruptSnapshot},
		{name: "header only", data: valid[:snapshotHeaderSize], want: ErrCorruptSnapshot},
		{name: "truncated", data: valid[:len(valid)-1], want: ErrCorruptSnapshot},
		{name: "extra byte", data: append(bytes.Clone(valid), 0), want: ErrCorruptSnapshot},
		{name: "bad magic", data: func() []byte { d := bytes.Clone(valid); d[0] = 'X'; return reseal(d) }(), want: ErrCorruptSnapshot},
		{name: "unsupported version", data: func() []byte { d := bytes.Clone(valid); d[len(snapshotMagic)] = 2; return reseal(d) }(), want: ErrUnsupportedFormat},
		{name: "flipped payload bit", data: func() []byte { d := bytes.Clone(valid); d[snapshotHeaderSize+20] ^= 0x10; return d }(), want: ErrCorruptSnapshot},
		{name: "flipped trailer bit", data: func() []byte { d := bytes.Clone(valid); d[len(d)-1] ^= 1; return d }(), want: ErrCorruptSnapshot},
		{name: "inflated key count", data: func() []byte {
			d := bytes.Clone(valid)
			binary.BigEndian.PutUint64(d[snapshotHeaderSize+8:], 1<<40)
			return reseal(d)
		}(), want: ErrCorruptSnapshot},
		{name: "key count beyond payload", data: buildSnapshot(3, good, nil)[:0], want: ErrCorruptSnapshot},
		{name: "key count larger than records", data: func() []byte {
			d := buildSnapshot(3, good, nil)
			binary.BigEndian.PutUint64(d[snapshotHeaderSize+8:], 3)
			return reseal(d)
		}(), want: ErrCorruptSnapshot},
		{name: "unsorted keys", data: buildSnapshot(3, []KeyValue{good[1], good[0]}, nil), want: ErrCorruptSnapshot},
		{name: "duplicate key", data: buildSnapshot(3, []KeyValue{good[0], good[0]}, nil), want: ErrCorruptSnapshot},
		{name: "empty key", data: buildSnapshot(3, []KeyValue{kv("", "1", 1, 1, 1)}, nil), want: ErrCorruptSnapshot},
		{name: "oversized key", data: buildSnapshot(3, []KeyValue{{Key: make([]byte, MaxKeySize+1), Version: 1, CreateRevision: 1, ModRevision: 1}}, nil), want: ErrCorruptSnapshot},
		{name: "oversized value", data: buildSnapshot(3, []KeyValue{{Key: []byte("k"), Value: make([]byte, MaxValueSize+1), Version: 1, CreateRevision: 1, ModRevision: 1}}, nil), want: ErrCorruptSnapshot},
		{name: "zero version", data: buildSnapshot(3, []KeyValue{kv("a", "1", 0, 1, 1)}, nil), want: ErrCorruptSnapshot},
		{name: "zero create revision", data: buildSnapshot(3, []KeyValue{kv("a", "1", 1, 0, 1)}, nil), want: ErrCorruptSnapshot},
		{name: "create after mod revision", data: buildSnapshot(3, []KeyValue{kv("a", "1", 1, 2, 1)}, nil), want: ErrCorruptSnapshot},
		{name: "mod revision beyond snapshot", data: buildSnapshot(3, []KeyValue{kv("a", "1", 1, 1, 4)}, nil), want: ErrCorruptSnapshot},
		{name: "session count beyond payload", data: func() []byte {
			d := buildSnapshot(3, nil, []sessionRecord{goodSession})
			binary.BigEndian.PutUint64(d[len(d)-snapshotTrailerSize-minSessionRecordSize-keyRecordMetaSize-2-8:], 1<<40)
			return reseal(d)
		}(), want: ErrCorruptSnapshot},
		{name: "zero client id", data: buildSnapshot(3, nil, []sessionRecord{{clientID: 0, revision: 1}}), want: ErrCorruptSnapshot},
		{name: "unsorted sessions", data: buildSnapshot(3, nil, []sessionRecord{{clientID: 2, revision: 1}, {clientID: 1, revision: 2}}), want: ErrCorruptSnapshot},
		{name: "duplicate sessions", data: buildSnapshot(3, nil, []sessionRecord{{clientID: 2, revision: 1}, {clientID: 2, revision: 2}}), want: ErrCorruptSnapshot},
		{name: "session revision beyond snapshot", data: buildSnapshot(3, nil, []sessionRecord{{clientID: 1, revision: 4}}), want: ErrCorruptSnapshot},
		{name: "unknown result flags", data: func() []byte {
			d := buildSnapshot(3, nil, []sessionRecord{{clientID: 1, revision: 1}})
			d[len(d)-snapshotTrailerSize-1] = 0x40
			return reseal(d)
		}(), want: ErrCorruptSnapshot},
		{name: "invalid key value in result", data: buildSnapshot(3, nil, []sessionRecord{{clientID: 1, revision: 1, result: Result{KV: ptr(kv("a", "1", 0, 1, 1))}}}), want: ErrCorruptSnapshot},
		{name: "trailing bytes inside body", data: func() []byte {
			d := buildSnapshot(3, good, nil)
			d = append(d[:len(d)-snapshotTrailerSize], 0)
			return seal(d)
		}(), want: ErrCorruptSnapshot},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dst := NewStore(Options{})
			mustApply(t, dst, 1, put("keep", "me"))
			w := dst.Watch([]byte("keep"), nil, 4)
			err := dst.Restore(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Restore error = %v, want %v", err, tc.want)
			}
			// A rejected snapshot leaves everything untouched, watchers included.
			assertGet(t, dst, kv("keep", "me", 1, 1, 1))
			if dst.Revision() != 1 || dst.Len() != 1 || dst.Watchers() != 1 {
				t.Fatalf("failed restore changed state: revision=%d len=%d watchers=%d", dst.Revision(), dst.Len(), dst.Watchers())
			}
			mustApply(t, dst, 2, put("keep", "still"))
			if ev := recvEvent(t, w); string(ev.KV.Value) != "still" {
				t.Fatalf("watcher after failed restore got %+v", ev)
			}
		})
	}
}

func TestRestoreAcceptsValidHandBuiltSnapshot(t *testing.T) {
	t.Parallel()
	data := buildSnapshot(10,
		[]KeyValue{kv("a", "1", 3, 1, 9), kv("b", "", 1, 2, 2)},
		[]sessionRecord{
			{clientID: 1, sequence: 4, revision: 9, result: Result{KV: ptr(kv("a", "1", 3, 1, 9)), PrevKV: ptr(kv("a", "0", 2, 1, 5))}},
			{clientID: 7, sequence: 1, revision: 10, result: Result{Deleted: true}},
		})
	s := NewStore(Options{})
	if err := s.Restore(data); err != nil {
		t.Fatal(err)
	}
	assertGet(t, s, kv("a", "1", 3, 1, 9))
	assertGet(t, s, kv("b", "", 1, 2, 2))
	again, _ := s.Snapshot()
	if !bytes.Equal(again, data) {
		t.Fatal("re-encoding a hand-built snapshot changed its bytes")
	}
	res := mustApply(t, s, 11, withSession(del("whatever"), 7, 1))
	if !res.Duplicate || !res.Deleted {
		t.Fatalf("restored delete result = %+v", res)
	}
}

// TestRestoreRandomCorruptionNeverPanics mutates a valid snapshot in random
// ways, with and without a valid checksum, and requires Restore to either
// reject the input or accept a state that re-encodes to the same bytes.
func TestRestoreRandomCorruptionNeverPanics(t *testing.T) {
	t.Parallel()
	src := NewStore(Options{})
	populate(t, src)
	valid, _ := src.Snapshot()
	rng := rand.New(rand.NewPCG(5, 6))
	accepted := 0
	for range 3000 {
		d := bytes.Clone(valid)
		switch rng.IntN(4) {
		case 0:
			d[rng.IntN(len(d))] ^= byte(1 << rng.IntN(8))
		case 1:
			d[rng.IntN(len(d))] = byte(rng.IntN(256))
			d = reseal(d)
		case 2:
			d = d[:rng.IntN(len(d))+1]
			if len(d) > snapshotTrailerSize {
				d = reseal(d)
			}
		default:
			i := rng.IntN(len(d))
			d = reseal(append(d[:i:i], d[i+1:]...))
		}
		s := NewStore(Options{})
		if err := s.Restore(d); err != nil {
			continue
		}
		accepted++
		again, _ := s.Snapshot()
		if !bytes.Equal(again, d) {
			t.Fatalf("accepted a mutated snapshot that does not re-encode identically")
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was accepted; the test did not exercise the success path")
	}
}

func FuzzRestore(f *testing.F) {
	src := NewStore(Options{})
	for i, c := range []Command{put("a", "1"), withPrevKV(withSession(put("a", "2"), 3, 1)), withSession(del("a"), 4, 1), cas("b", "x", 0)} {
		src.Apply(uint64(i+1), c)
	}
	valid, _ := src.Snapshot()
	f.Add(valid)
	f.Add(reseal(valid[:len(valid)-9]))
	f.Add([]byte(snapshotMagic))
	f.Add(append([]byte(snapshotMagic), snapshotFormatVersion, 0, 0, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		s := NewStore(Options{})
		if err := s.Restore(data); err != nil {
			if !errors.Is(err, ErrCorruptSnapshot) && !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("unexpected error class: %v", err)
			}
			return
		}
		again, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, data) {
			t.Fatal("accepted snapshot does not re-encode identically")
		}
	})
}
