package raft

import (
	"errors"
	"testing"
)

func TestMemoryStorageAppendAndEntries(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStorage()
	if err := ms.Append(ents(1, 1, 2, 1, 3, 2)); err != nil {
		t.Fatal(err)
	}
	first, _ := ms.FirstIndex()
	last, _ := ms.LastIndex()
	if first != 1 || last != 3 {
		t.Fatalf("first/last = %d/%d", first, last)
	}
	got, err := ms.Entries(1, 4, 1<<20)
	if err != nil || entriesString(got) != entriesString(ents(1, 1, 2, 1, 3, 2)) {
		t.Fatalf("entries = %s, %v", entriesString(got), err)
	}
	if _, err := ms.Entries(0, 2, 1<<20); !errors.Is(err, ErrCompacted) {
		t.Fatalf("entries from 0: err = %v", err)
	}
	if _, err := ms.Entries(2, 9, 1<<20); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("entries beyond end: err = %v", err)
	}
	// Overwrite with a conflicting suffix.
	if err := ms.Append(ents(2, 5, 3, 5, 4, 5)); err != nil {
		t.Fatal(err)
	}
	got, _ = ms.Entries(1, 5, 1<<20)
	if entriesString(got) != entriesString(ents(1, 1, 2, 5, 3, 5, 4, 5)) {
		t.Fatalf("after overwrite = %s", entriesString(got))
	}
	// A gap is an error.
	if err := ms.Append(ents(9, 9)); err == nil {
		t.Fatal("append with a gap succeeded")
	}
	// Size limiting always returns at least one entry.
	got, _ = ms.Entries(1, 5, 1)
	if len(got) != 1 {
		t.Fatalf("size-limited entries = %d", len(got))
	}
}

func TestMemoryStorageSnapshotAndCompact(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStorage()
	if err := ms.Append(ents(1, 1, 2, 1, 3, 2, 4, 2, 5, 3)); err != nil {
		t.Fatal(err)
	}
	cs := ConfState{Voters: []NodeID{1, 2, 3}}
	snap, err := ms.CreateSnapshot(4, cs, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Meta.Index != 4 || snap.Meta.Term != 2 || !snap.Meta.ConfState.Equal(cs) {
		t.Fatalf("snapshot meta = %+v", snap.Meta)
	}
	if _, err := ms.CreateSnapshot(3, cs, nil); !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Fatalf("older snapshot: err = %v", err)
	}
	if _, err := ms.CreateSnapshot(9, cs, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("future snapshot: err = %v", err)
	}

	if err := ms.Compact(4); err != nil {
		t.Fatal(err)
	}
	first, _ := ms.FirstIndex()
	last, _ := ms.LastIndex()
	if first != 5 || last != 5 {
		t.Fatalf("after compact first/last = %d/%d", first, last)
	}
	if term, err := ms.Term(4); err != nil || term != 2 {
		t.Fatalf("term of compaction point = %d, %v", term, err)
	}
	if _, err := ms.Term(3); !errors.Is(err, ErrCompacted) {
		t.Fatalf("term before compaction: err = %v", err)
	}
	if err := ms.Compact(4); !errors.Is(err, ErrCompacted) {
		t.Fatalf("double compact: err = %v", err)
	}
	if err := ms.Compact(9); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("compact beyond end: err = %v", err)
	}

	// Appending entries older than the snapshot is a no-op; overlapping
	// ones are trimmed.
	if err := ms.Append(ents(2, 1, 3, 2)); err != nil {
		t.Fatal(err)
	}
	if err := ms.Append(ents(4, 2, 5, 3, 6, 3)); err != nil {
		t.Fatal(err)
	}
	last, _ = ms.LastIndex()
	if last != 6 {
		t.Fatalf("last after trimmed append = %d", last)
	}

	// ApplySnapshot resets the log.
	if err := ms.ApplySnapshot(Snapshot{Meta: SnapshotMeta{Index: 20, Term: 5, ConfState: cs}}); err != nil {
		t.Fatal(err)
	}
	first, _ = ms.FirstIndex()
	last, _ = ms.LastIndex()
	if first != 21 || last != 20 {
		t.Fatalf("after apply snapshot first/last = %d/%d", first, last)
	}
	if err := ms.ApplySnapshot(Snapshot{Meta: SnapshotMeta{Index: 10, Term: 5}}); !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Fatalf("older ApplySnapshot: err = %v", err)
	}
	hs, got, err := ms.InitialState()
	if err != nil || !got.Equal(cs) || !hs.IsEmpty() {
		t.Fatalf("initial state = %+v, %s, %v", hs, got, err)
	}
	ms.SetHardState(HardState{Term: 5, Vote: 2, Commit: 20})
	if got := ms.HardState(); got != (HardState{Term: 5, Vote: 2, Commit: 20}) {
		t.Fatalf("hard state = %+v", got)
	}
}
