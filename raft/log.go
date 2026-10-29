package raft

import (
	"errors"
	"fmt"
	"log/slog"
)

// raftLog is the node's view of the replicated log. It stitches together the
// stable entries held by Storage with the entries that have been appended but
// not yet persisted by the application (the "unstable" tail), and tracks the
// commit and apply cursors.
//
//	      stable (Storage)              unstable
//	┌──────────────────────────┬──────────────────────┐
//	│ snapshot │ e1 e2 ... eN  │ eN+1 ... eM          │
//	└──────────────────────────┴──────────────────────┘
//	firstIndex-1              unstableOffset = N+1     lastIndex = M
//
// A snapshot received from the leader sits in pendingSnapshot until the
// application has persisted it; while it is pending, it defines the start of
// the log and no committed entries are handed out.
type raftLog struct {
	storage Storage

	// unstable holds entries at indexes [unstableOffset, unstableOffset+len).
	unstable       []Entry
	unstableOffset Index

	// pendingSnapshot is a snapshot that must be persisted before any entry
	// after it can be used.
	pendingSnapshot *Snapshot

	// committed is the highest index known to be committed by a quorum.
	committed Index
	// applied is the highest index the application has applied. It is always
	// at most committed.
	applied Index

	maxCommittedSizePerReady uint64
	logger                   *slog.Logger
}

func newLog(storage Storage, logger *slog.Logger, maxCommittedSizePerReady uint64) (*raftLog, error) {
	first, err := storage.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("storage first index: %w", err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("storage last index: %w", err)
	}
	if last+1 < first {
		return nil, fmt.Errorf("%w: storage reports last index %d before first index %d", ErrInvalidConfig, last, first)
	}
	// Everything covered by the snapshot is committed by definition, even if
	// a commit-only hard state update was lost before it was synced.
	snap, err := storage.Snapshot()
	if err != nil && !errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
		return nil, fmt.Errorf("storage snapshot: %w", err)
	}
	committed := max(first-1, snap.Meta.Index)
	if committed > last {
		return nil, fmt.Errorf("%w: snapshot index %d is beyond last index %d", ErrInvalidConfig, snap.Meta.Index, last)
	}
	return &raftLog{
		storage:                  storage,
		unstableOffset:           last + 1,
		committed:                committed,
		applied:                  committed,
		maxCommittedSizePerReady: maxCommittedSizePerReady,
		logger:                   logger,
	}, nil
}

// firstIndex returns the index of the first entry that can still be read.
func (l *raftLog) firstIndex() Index {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Meta.Index + 1
	}
	first, err := l.storage.FirstIndex()
	if err != nil {
		panic(fmt.Sprintf("raft: storage first index: %v", err))
	}
	return first
}

// lastIndex returns the index of the last entry in the log, stable or not.
func (l *raftLog) lastIndex() Index {
	if n := len(l.unstable); n > 0 {
		return l.unstableOffset + Index(n) - 1
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Meta.Index
	}
	last, err := l.storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("raft: storage last index: %v", err))
	}
	return last
}

func (l *raftLog) lastTerm() Term {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("raft: term of last index: %v", err))
	}
	return t
}

// term returns the term of the entry at index i. The index just before
// firstIndex (the snapshot position) is valid.
func (l *raftLog) term(i Index) (Term, error) {
	dummy := l.firstIndex() - 1
	if i < dummy {
		return 0, ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	if n := Index(len(l.unstable)); n > 0 && i >= l.unstableOffset && i < l.unstableOffset+n {
		return l.unstable[i-l.unstableOffset].Term, nil
	}
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Meta.Index {
		return l.pendingSnapshot.Meta.Term, nil
	}
	t, err := l.storage.Term(i)
	if err != nil {
		if errors.Is(err, ErrCompacted) || errors.Is(err, ErrUnavailable) {
			return 0, err
		}
		panic(fmt.Sprintf("raft: storage term(%d): %v", i, err))
	}
	return t, nil
}

// zeroTermOnOutOfBounds converts the storage errors into a zero term, which
// callers use when they only need to compare terms.
func (l *raftLog) zeroTermOnOutOfBounds(t Term, err error) Term {
	if err == nil {
		return t
	}
	if errors.Is(err, ErrCompacted) || errors.Is(err, ErrUnavailable) {
		return 0
	}
	panic(fmt.Sprintf("raft: unexpected log error: %v", err))
}

func (l *raftLog) matchTerm(i Index, t Term) bool {
	got, err := l.term(i)
	if err != nil {
		return false
	}
	return got == t
}

// isUpToDate implements the election restriction of §5.4.1: a candidate's log
// is at least as up to date as ours if its last term is higher, or equal with
// an index at least as large.
func (l *raftLog) isUpToDate(lastIndex Index, lastTerm Term) bool {
	ourTerm := l.lastTerm()
	return lastTerm > ourTerm || (lastTerm == ourTerm && lastIndex >= l.lastIndex())
}

// append adds entries to the unstable tail and returns the new last index.
// Entries must start after the commit index; overwriting committed entries is
// a safety violation and therefore a bug, so it panics.
func (l *raftLog) append(ents ...Entry) Index {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		panic(fmt.Sprintf("raft: append after %d would overwrite committed entries (commit %d)", after, l.committed))
	}
	l.truncateAndAppend(ents)
	return l.lastIndex()
}

func (l *raftLog) truncateAndAppend(ents []Entry) {
	after := ents[0].Index
	switch {
	case after == l.unstableOffset+Index(len(l.unstable)):
		// Directly after the current tail: plain append.
		l.unstable = append(l.unstable, ents...)
	case after <= l.unstableOffset:
		// The new entries replace the whole unstable tail and possibly some
		// stable entries; the application's storage handles the stable part
		// when it persists the entries from the next Ready.
		l.logger.Debug("replacing unstable entries", "from", after, "unstable_offset", l.unstableOffset)
		l.unstableOffset = after
		l.unstable = append([]Entry(nil), ents...)
	default:
		// Truncate part of the unstable tail, then append.
		l.logger.Debug("truncating unstable entries", "from", after)
		l.unstable = append(l.unstable[:after-l.unstableOffset:after-l.unstableOffset], ents...)
	}
}

// maybeAppend implements the AppendEntries consistency check. If the entry at
// prevIndex has term prevTerm, it appends the entries that are new, advances
// the commit index to min(commit, last new index), and returns the index of
// the last new entry. Otherwise it returns ok=false.
func (l *raftLog) maybeAppend(prevIndex Index, prevTerm Term, commit Index, ents []Entry) (lastNew Index, ok bool) {
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew = prevIndex + Index(len(ents))
	ci := l.findConflict(ents)
	switch {
	case ci == 0:
		// Every entry already matches; nothing to append.
	case ci <= l.committed:
		// A committed entry can only be contradicted by a leader that
		// violates Raft's invariants or by a hostile peer. Refuse the append
		// rather than crash: the local log is the trustworthy side.
		l.logger.Error("refusing append that conflicts with a committed entry",
			"index", ci, "committed", l.committed)
		return 0, false
	default:
		offset := prevIndex + 1
		l.append(ents[ci-offset:]...)
	}
	l.commitTo(min(commit, lastNew))
	return lastNew, true
}

// findConflict returns the index of the first entry in ents that conflicts
// with the local log (same index, different term), or the first index beyond
// the local log if ents extends it, or zero if every entry already matches.
func (l *raftLog) findConflict(ents []Entry) Index {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			if e.Index <= l.lastIndex() {
				l.logger.Debug("found log conflict",
					"index", e.Index,
					"existing_term", l.zeroTermOnOutOfBounds(l.term(e.Index)),
					"conflicting_term", e.Term)
			}
			return e.Index
		}
	}
	return 0
}

// conflictHint computes the accelerated-backtracking hint a follower returns
// when it rejects an AppendEntries at prevIndex. If the follower's log is too
// short, the hint is (0, lastIndex+1). Otherwise it is the term of the
// follower's entry at prevIndex together with the first index of that term.
func (l *raftLog) conflictHint(prevIndex Index) (conflictTerm Term, conflictIndex Index) {
	last := l.lastIndex()
	if prevIndex > last {
		return 0, last + 1
	}
	t, err := l.term(prevIndex)
	if err != nil {
		// The entry has been compacted; the leader should send a snapshot or
		// probe further back. Reporting the first index steers it there.
		return 0, l.firstIndex()
	}
	i := prevIndex
	for i > l.firstIndex() {
		pt, err := l.term(i - 1)
		if err != nil || pt != t {
			break
		}
		i--
	}
	return t, i
}

// nextIndexAfterReject is the leader-side half of accelerated backtracking.
// Given a follower's hint, it returns the next index to probe: just after the
// leader's last entry of conflictTerm if it has that term, else conflictIndex.
func (l *raftLog) nextIndexAfterReject(prevIndex Index, conflictTerm Term, conflictIndex Index) Index {
	if conflictTerm == 0 {
		return conflictIndex
	}
	// Terms in a log are non-decreasing, so walk back from prevIndex until the
	// term drops to or below conflictTerm.
	i := min(prevIndex, l.lastIndex())
	for i >= l.firstIndex() {
		t, err := l.term(i)
		if err != nil {
			break
		}
		if t == conflictTerm {
			return i + 1
		}
		if t < conflictTerm {
			break
		}
		i--
	}
	return conflictIndex
}

// commitTo advances the commit index. The index must exist in the log.
func (l *raftLog) commitTo(c Index) {
	if c <= l.committed {
		return
	}
	if l.lastIndex() < c {
		panic(fmt.Sprintf("raft: commit index %d is beyond last index %d; was the log corrupted?", c, l.lastIndex()))
	}
	l.committed = c
}

// appliedTo records that the application has applied up to index i.
func (l *raftLog) appliedTo(i Index) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		panic(fmt.Sprintf("raft: applied(%d) is out of range [prev applied %d, committed %d]", i, l.applied, l.committed))
	}
	l.applied = i
}

// stableTo marks unstable entries up to (i, t) as persisted. A term mismatch
// means the tail was replaced after the Ready was produced, in which case the
// entries remain unstable and will be delivered again.
func (l *raftLog) stableTo(i Index, t Term) {
	n := Index(len(l.unstable))
	if n == 0 || i < l.unstableOffset || i >= l.unstableOffset+n {
		return
	}
	if l.unstable[i-l.unstableOffset].Term != t {
		return
	}
	l.unstable = append([]Entry(nil), l.unstable[i+1-l.unstableOffset:]...)
	l.unstableOffset = i + 1
}

// stableSnapTo clears the pending snapshot once the application persisted it.
func (l *raftLog) stableSnapTo(i Index) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Meta.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *raftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil
}

// restore resets the log to start at the given snapshot.
func (l *raftLog) restore(s Snapshot) {
	l.logger.Info("restoring log from snapshot",
		"snapshot_index", s.Meta.Index, "snapshot_term", s.Meta.Term,
		"committed", l.committed, "last_index", l.lastIndex())
	l.committed = s.Meta.Index
	l.unstable = nil
	l.unstableOffset = s.Meta.Index + 1
	l.pendingSnapshot = &s
}

// snapshot returns the pending snapshot if any, otherwise the stored one.
func (l *raftLog) snapshot() (Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// unstableEntries returns a copy of the unstable tail.
func (l *raftLog) unstableEntries() []Entry {
	if len(l.unstable) == 0 {
		return nil
	}
	return append([]Entry(nil), l.unstable...)
}

// hasNextCommittedEnts reports whether committed, stable entries are waiting
// to be applied.
func (l *raftLog) hasNextCommittedEnts() bool {
	if l.hasPendingSnapshot() {
		return false
	}
	return l.applied < l.stableCommitted()
}

// stableCommitted is the highest committed index that is also stable. Only
// stable entries are handed to the application, so that a crash after apply
// cannot lose an entry that was acknowledged.
func (l *raftLog) stableCommitted() Index {
	return min(l.committed, l.unstableOffset-1)
}

// nextCommittedEnts returns the committed entries that can be applied now,
// bounded by maxCommittedSizePerReady.
func (l *raftLog) nextCommittedEnts() []Entry {
	if !l.hasNextCommittedEnts() {
		return nil
	}
	ents, err := l.slice(l.applied+1, l.stableCommitted()+1, l.maxCommittedSizePerReady)
	if err != nil {
		panic(fmt.Sprintf("raft: committed entries [%d, %d] unavailable: %v", l.applied+1, l.stableCommitted(), err))
	}
	return ents
}

// entries returns entries from index i to the end, bounded by maxSize.
func (l *raftLog) entries(i Index, maxSize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

// slice returns entries in [lo, hi), bounded by maxSize but never empty when
// the range is non-empty.
func (l *raftLog) slice(lo, hi Index, maxSize uint64) ([]Entry, error) {
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid slice %d > %d", lo, hi)
	}
	if lo == hi {
		return nil, nil
	}
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}

	var ents []Entry
	if lo < l.unstableOffset {
		stored, err := l.storage.Entries(lo, min(hi, l.unstableOffset), maxSize)
		if err != nil {
			if errors.Is(err, ErrCompacted) || errors.Is(err, ErrUnavailable) {
				return nil, err
			}
			panic(fmt.Sprintf("raft: storage entries [%d, %d): %v", lo, min(hi, l.unstableOffset), err))
		}
		// Storage may have truncated because of maxSize; stop here if so.
		if Index(len(stored)) < min(hi, l.unstableOffset)-lo {
			return stored, nil
		}
		ents = stored
	}
	if hi > l.unstableOffset {
		from := max(lo, l.unstableOffset)
		unstable := l.unstable[from-l.unstableOffset : hi-l.unstableOffset]
		if len(ents) > 0 {
			ents = append(append([]Entry(nil), ents...), unstable...)
		} else {
			ents = append([]Entry(nil), unstable...)
		}
	}
	return limitSize(ents, maxSize), nil
}

// allEntries returns every entry still held in the log; used by tests.
func (l *raftLog) allEntries() []Entry {
	ents, err := l.entries(l.firstIndex(), ^uint64(0))
	if err == nil {
		return ents
	}
	if errors.Is(err, ErrCompacted) {
		return l.allEntries() // compaction raced; retry
	}
	panic(fmt.Sprintf("raft: all entries: %v", err))
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d applied=%d unstable.offset=%d unstable.len=%d", l.committed, l.applied, l.unstableOffset, len(l.unstable))
}
