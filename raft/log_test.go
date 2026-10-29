package raft

import (
	"errors"
	"testing"
)

func newTestLog(t *testing.T, stable []Entry, snapIndex, snapTerm Index) (*raftLog, *MemoryStorage) {
	t.Helper()
	ms := NewMemoryStorage()
	if snapIndex > 0 {
		if err := ms.ApplySnapshot(Snapshot{Meta: SnapshotMeta{Index: snapIndex, Term: Term(snapTerm)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.Append(stable); err != nil {
		t.Fatal(err)
	}
	l, err := newLog(ms, testLogger(t), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return l, ms
}

func ents(pairs ...uint64) []Entry {
	if len(pairs)%2 != 0 {
		panic("pairs must be index/term couples")
	}
	out := make([]Entry, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, Entry{Index: Index(pairs[i]), Term: Term(pairs[i+1])})
	}
	return out
}

func TestLogTermAndBounds(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, ents(1, 1, 2, 1, 3, 2), 0, 0)
	l.append(ents(4, 3)...)

	cases := []struct {
		index Index
		term  Term
		err   error
	}{
		{0, 0, nil},
		{1, 1, nil},
		{3, 2, nil},
		{4, 3, nil},
		{5, 0, ErrUnavailable},
	}
	for _, tc := range cases {
		got, err := l.term(tc.index)
		if !errors.Is(err, tc.err) || got != tc.term {
			t.Errorf("term(%d) = %d, %v; want %d, %v", tc.index, got, err, tc.term, tc.err)
		}
	}
	if l.lastIndex() != 4 || l.lastTerm() != 3 || l.firstIndex() != 1 {
		t.Fatalf("last=%d lastTerm=%d first=%d", l.lastIndex(), l.lastTerm(), l.firstIndex())
	}
	if !l.isUpToDate(4, 3) || !l.isUpToDate(1, 4) || l.isUpToDate(3, 3) || l.isUpToDate(10, 2) {
		t.Fatal("isUpToDate disagrees with §5.4.1")
	}
}

func TestLogCompactedTerm(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, ents(6, 3, 7, 3), 5, 2)
	if _, err := l.term(4); !errors.Is(err, ErrCompacted) {
		t.Fatalf("term(4) err = %v, want ErrCompacted", err)
	}
	if got, err := l.term(5); err != nil || got != 2 {
		t.Fatalf("term(5) = %d, %v; want snapshot term 2", got, err)
	}
	if l.firstIndex() != 6 {
		t.Fatalf("first index = %d, want 6", l.firstIndex())
	}
}

func TestLogMaybeAppend(t *testing.T) {
	t.Parallel()
	base := ents(1, 1, 2, 2, 3, 3)
	cases := []struct {
		name      string
		prevIndex Index
		prevTerm  Term
		commit    Index
		ents      []Entry
		ok        bool
		wantLast  Index
		wantLog   []Entry
		wantCommt Index
	}{
		{"mismatch term", 3, 2, 3, nil, false, 0, base, 1},
		{"beyond end", 4, 3, 3, nil, false, 0, base, 1},
		{"match no entries", 3, 3, 3, nil, true, 3, base, 3},
		{"match commit capped", 3, 3, 10, nil, true, 3, base, 3},
		{"append new", 3, 3, 4, ents(4, 4), true, 4, ents(1, 1, 2, 2, 3, 3, 4, 4), 4},
		{"duplicate entries", 1, 1, 3, ents(2, 2, 3, 3), true, 3, base, 3},
		{"conflict truncates", 1, 1, 3, ents(2, 2, 3, 4, 4, 4), true, 4, ents(1, 1, 2, 2, 3, 4, 4, 4), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l, _ := newTestLog(t, base, 0, 0)
			l.committed = 1
			last, ok := l.maybeAppend(tc.prevIndex, tc.prevTerm, tc.commit, tc.ents)
			if ok != tc.ok || last != tc.wantLast {
				t.Fatalf("maybeAppend = %d, %t; want %d, %t", last, ok, tc.wantLast, tc.ok)
			}
			if got := entriesString(l.allEntries()); got != entriesString(tc.wantLog) {
				t.Fatalf("log = %s, want %s", got, entriesString(tc.wantLog))
			}
			if l.committed != tc.wantCommt {
				t.Fatalf("committed = %d, want %d", l.committed, tc.wantCommt)
			}
		})
	}
}

func TestLogConflictOnCommittedEntryIsRefused(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, ents(1, 1, 2, 2, 3, 3), 0, 0)
	l.committed = 3
	if _, ok := l.maybeAppend(1, 1, 3, ents(2, 5)); ok {
		t.Fatal("append that contradicts a committed entry was accepted")
	}
	if got := entriesString(l.allEntries()); got != entriesString(ents(1, 1, 2, 2, 3, 3)) {
		t.Fatalf("log changed: %s", got)
	}
}

func TestLogConflictHints(t *testing.T) {
	t.Parallel()
	// Follower log terms: 1 1 2 2 2 3 at indexes 1..6, snapshot at 0.
	l, _ := newTestLog(t, ents(1, 1, 2, 1, 3, 2, 4, 2, 5, 2, 6, 3), 0, 0)
	cases := []struct {
		prev      Index
		wantTerm  Term
		wantIndex Index
	}{
		{9, 0, 7}, // too short
		{6, 3, 6},
		{5, 2, 3}, // first index of term 2
		{2, 1, 1},
	}
	for _, tc := range cases {
		term, idx := l.conflictHint(tc.prev)
		if term != tc.wantTerm || idx != tc.wantIndex {
			t.Errorf("conflictHint(%d) = %d/%d, want %d/%d", tc.prev, term, idx, tc.wantTerm, tc.wantIndex)
		}
	}

	// Leader log terms: 1 1 2 4 4 4 at indexes 1..6.
	leader, _ := newTestLog(t, ents(1, 1, 2, 1, 3, 2, 4, 4, 5, 4, 6, 4), 0, 0)
	leaderCases := []struct {
		prev          Index
		conflictTerm  Term
		conflictIndex Index
		want          Index
	}{
		{6, 0, 4, 4}, // follower short: probe at its end
		{5, 2, 3, 4}, // leader has term 2 ending at 3: resume at 4
		{5, 3, 5, 5}, // leader lacks term 3: use follower's first index of it
		{3, 1, 1, 3}, // leader has term 1 ending at 2
		{6, 9, 6, 6}, // unknown higher term: fall back to conflict index
	}
	for _, tc := range leaderCases {
		if got := leader.nextIndexAfterReject(tc.prev, tc.conflictTerm, tc.conflictIndex); got != tc.want {
			t.Errorf("nextIndexAfterReject(%d, %d, %d) = %d, want %d", tc.prev, tc.conflictTerm, tc.conflictIndex, got, tc.want)
		}
	}
}

func TestLogUnstableAndStableTo(t *testing.T) {
	t.Parallel()
	l, ms := newTestLog(t, ents(1, 1, 2, 1), 0, 0)
	l.append(ents(3, 2, 4, 2)...)
	if got := entriesString(l.unstableEntries()); got != entriesString(ents(3, 2, 4, 2)) {
		t.Fatalf("unstable = %s", got)
	}
	// A term mismatch leaves the entries unstable.
	l.stableTo(4, 9)
	if len(l.unstable) != 2 {
		t.Fatalf("stableTo with wrong term changed unstable: %d", len(l.unstable))
	}
	if err := ms.Append(l.unstableEntries()); err != nil {
		t.Fatal(err)
	}
	l.stableTo(4, 2)
	if len(l.unstable) != 0 || l.unstableOffset != 5 {
		t.Fatalf("after stableTo: unstable=%d offset=%d", len(l.unstable), l.unstableOffset)
	}
	// Overwriting the stable tail.
	l.append(ents(3, 5)...)
	if l.unstableOffset != 3 || l.lastIndex() != 3 || l.lastTerm() != 5 {
		t.Fatalf("after overwrite: offset=%d last=%d term=%d", l.unstableOffset, l.lastIndex(), l.lastTerm())
	}
	got, err := l.slice(1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if s := entriesString(got); s != entriesString(ents(1, 1, 2, 1, 3, 5)) {
		t.Fatalf("slice across stable/unstable = %s", s)
	}
}

func TestLogCommittedEntriesOnlyWhenStable(t *testing.T) {
	t.Parallel()
	l, ms := newTestLog(t, ents(1, 1), 0, 0)
	l.append(ents(2, 1, 3, 1)...)
	l.commitTo(3)
	if l.hasNextCommittedEnts() {
		got := l.nextCommittedEnts()
		if len(got) != 1 || got[0].Index != 1 {
			t.Fatalf("unstable entries handed out for apply: %s", entriesString(got))
		}
	}
	if err := ms.Append(l.unstableEntries()); err != nil {
		t.Fatal(err)
	}
	l.stableTo(3, 1)
	got := l.nextCommittedEnts()
	if entriesString(got) != entriesString(ents(1, 1, 2, 1, 3, 1)) {
		t.Fatalf("committed entries = %s", entriesString(got))
	}
	l.appliedTo(3)
	if l.hasNextCommittedEnts() {
		t.Fatal("entries still pending after appliedTo")
	}
}

func TestLogMaxCommittedSizePerReady(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStorage()
	big := []Entry{
		{Index: 1, Term: 1, Data: make([]byte, 100)},
		{Index: 2, Term: 1, Data: make([]byte, 100)},
		{Index: 3, Term: 1, Data: make([]byte, 100)},
	}
	if err := ms.Append(big); err != nil {
		t.Fatal(err)
	}
	l, err := newLog(ms, testLogger(t), 150)
	if err != nil {
		t.Fatal(err)
	}
	l.commitTo(3)
	if got := l.nextCommittedEnts(); len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (size cap)", len(got))
	}
	l.appliedTo(1)
	if got := l.nextCommittedEnts(); len(got) != 1 || got[0].Index != 2 {
		t.Fatalf("second batch = %s", entriesString(got))
	}
}

func TestLogRestore(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, ents(1, 1, 2, 1), 0, 0)
	l.commitTo(1)
	l.restore(Snapshot{Meta: SnapshotMeta{Index: 10, Term: 4}})
	if !l.hasPendingSnapshot() || l.firstIndex() != 11 || l.lastIndex() != 10 || l.committed != 10 {
		t.Fatalf("after restore: first=%d last=%d committed=%d", l.firstIndex(), l.lastIndex(), l.committed)
	}
	if got, err := l.term(10); err != nil || got != 4 {
		t.Fatalf("term(10) = %d, %v", got, err)
	}
	if l.hasNextCommittedEnts() {
		t.Fatal("committed entries offered while a snapshot is pending")
	}
	l.append(ents(11, 4)...)
	l.stableSnapTo(10)
	if l.hasPendingSnapshot() {
		t.Fatal("snapshot still pending")
	}
}
