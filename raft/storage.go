package raft

import (
	"fmt"
	"sync"
)

// Storage is the read side of the durable state the library depends on.
//
// Writes happen in the application, in response to [Ready], which lets the
// application batch appends and decide when to fsync. Implementations must be
// safe for concurrent use if the application reads them from other goroutines;
// the library itself only calls them from the goroutine that drives the
// [RawNode].
//
// The log is addressed as a contiguous range [FirstIndex, LastIndex]. Entries
// below FirstIndex have been compacted into the snapshot; requests for them
// return [ErrCompacted]. Requests beyond LastIndex return [ErrUnavailable].
type Storage interface {
	// InitialState returns the persisted HardState and the membership recorded
	// in the latest snapshot (or an empty ConfState for a brand-new node).
	InitialState() (HardState, ConfState, error)

	// Entries returns entries in the half-open range [lo, hi). The result is
	// truncated so that the cumulative Entry.Size stays within maxSize, but
	// at least one entry is returned when the range is non-empty.
	Entries(lo, hi Index, maxSize uint64) ([]Entry, error)

	// Term returns the term of the entry at index i. The index of the latest
	// snapshot is valid and returns the snapshot's term, even though the entry
	// itself has been compacted.
	Term(i Index) (Term, error)

	// FirstIndex returns the index of the first entry still held in the log,
	// which is the snapshot index plus one.
	FirstIndex() (Index, error)

	// LastIndex returns the index of the last entry in the log, or the
	// snapshot index when the log is empty.
	LastIndex() (Index, error)

	// Snapshot returns the most recent snapshot. It may return
	// [ErrSnapshotTemporarilyUnavailable] while one is being produced.
	Snapshot() (Snapshot, error)
}

// MemoryStorage is an in-memory [Storage] that also exposes the write
// operations an application needs. It is used directly by tests and
// simulations, and as the in-memory index of the durable storage in the
// kvorum server, which replays its write-ahead log into a MemoryStorage on
// start-up.
//
// Internally ents[0] is a sentinel holding the index and term of the latest
// snapshot, so that ents[i] has index snapshot.Index + i.
type MemoryStorage struct {
	mu        sync.Mutex
	hardState HardState
	snapshot  Snapshot
	ents      []Entry
}

// NewMemoryStorage returns an empty storage: no entries, no snapshot, zero
// hard state.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{ents: []Entry{{}}}
}

// InitialState implements Storage.
func (ms *MemoryStorage) InitialState() (HardState, ConfState, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.hardState, ms.snapshot.Meta.ConfState.Clone(), nil
}

// SetHardState persists the hard state.
func (ms *MemoryStorage) SetHardState(hs HardState) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.hardState = hs
}

// HardState returns the stored hard state.
func (ms *MemoryStorage) HardState() HardState {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.hardState
}

// Entries implements Storage.
func (ms *MemoryStorage) Entries(lo, hi Index, maxSize uint64) ([]Entry, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndexLocked()+1 {
		return nil, ErrUnavailable
	}
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid range [%d, %d)", lo, hi)
	}
	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// Term implements Storage.
func (ms *MemoryStorage) Term(i Index) (Term, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// FirstIndex implements Storage.
func (ms *MemoryStorage) FirstIndex() (Index, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.ents[0].Index + 1, nil
}

// LastIndex implements Storage.
func (ms *MemoryStorage) LastIndex() (Index, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.lastIndexLocked(), nil
}

func (ms *MemoryStorage) lastIndexLocked() Index {
	return ms.ents[0].Index + Index(len(ms.ents)) - 1
}

// Snapshot implements Storage.
func (ms *MemoryStorage) Snapshot() (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot replaces the log with the given snapshot. It returns
// [ErrSnapshotOutOfDate] if the storage already holds a newer snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if snap.Meta.Index < ms.snapshot.Meta.Index {
		return ErrSnapshotOutOfDate
	}
	ms.snapshot = snap
	ms.ents = []Entry{{Index: snap.Meta.Index, Term: snap.Meta.Term}}
	return nil
}

// CreateSnapshot records a snapshot taken at index i with the given
// membership and application data, and returns it. The log is not compacted;
// call [MemoryStorage.Compact] for that. It fails with [ErrSnapshotOutOfDate]
// if i is not newer than the current snapshot and with [ErrUnavailable] if i
// is beyond the last index.
func (ms *MemoryStorage) CreateSnapshot(i Index, cs ConfState, data []byte) (Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if i <= ms.snapshot.Meta.Index {
		return Snapshot{}, ErrSnapshotOutOfDate
	}
	offset := ms.ents[0].Index
	if i > ms.lastIndexLocked() {
		return Snapshot{}, ErrUnavailable
	}
	ms.snapshot = Snapshot{
		Meta: SnapshotMeta{Index: i, Term: ms.ents[i-offset].Term, ConfState: cs.Clone()},
		Data: data,
	}
	return ms.snapshot, nil
}

// Compact discards all entries before compactIndex. The entry at
// compactIndex is retained as the new sentinel so its term remains known.
func (ms *MemoryStorage) Compact(compactIndex Index) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndexLocked() {
		return ErrUnavailable
	}
	i := int(compactIndex - offset)
	ents := make([]Entry, 1, len(ms.ents)-i+1)
	ents[0] = Entry{Index: ms.ents[i].Index, Term: ms.ents[i].Term}
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append adds entries to the log. Entries that overlap already-stored
// positions overwrite them, truncating everything after the last appended
// entry; entries older than the snapshot are ignored.
func (ms *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	first := ms.ents[0].Index + 1
	last := entries[0].Index + Index(len(entries)) - 1
	if last < first {
		return nil // entirely compacted already
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case Index(len(ms.ents)) > offset:
		ms.ents = append(ms.ents[:offset], entries...)
	case Index(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		return fmt.Errorf("raft: missing log entries [last: %d, append at: %d]", ms.lastIndexLocked(), entries[0].Index)
	}
	return nil
}

// limitSize truncates ents so that the cumulative size does not exceed
// maxSize, always keeping at least one entry.
func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var n int
	for n = 1; n < len(ents); n++ {
		size += ents[n].Size()
		if size > maxSize {
			break
		}
	}
	return ents[:n]
}
