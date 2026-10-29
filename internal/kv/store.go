package kv

import (
	"bytes"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/sadrax4/kvorum/internal/kv/ordered"
)

const (
	// DefaultMaxSessions is the session table bound used when Options.MaxSessions
	// is not positive.
	DefaultMaxSessions = 10000
	// DefaultWatchBuffer is the watcher channel capacity used when Store.Watch
	// is called with a buffer that is not positive.
	DefaultWatchBuffer = 128
)

// Options configures a Store.
type Options struct {
	// MaxSessions bounds the exactly-once session table. When the table is
	// full, the client whose most recent command has the lowest revision is
	// evicted. Zero or negative selects DefaultMaxSessions.
	MaxSessions int
	// Logger receives diagnostics such as watcher cancellations and snapshot
	// restores. Nil discards them.
	Logger *slog.Logger
}

// Store is the deterministic key-value state machine. It is safe for
// concurrent use; see the package documentation for the locking discipline.
// The zero value is not usable; create stores with NewStore.
type Store struct {
	mu          sync.RWMutex
	logger      *slog.Logger
	maxSessions int

	revision uint64
	index    *ordered.Map[*entry]
	sessions *sessionTable

	// keyWatchers indexes single-key watchers by their key; rangeWatchers holds
	// the rest. scratch is reused by dispatch to collect the watchers an event
	// matches, so that Apply does not allocate when nobody is watching.
	keyWatchers   map[string][]*Watcher
	rangeWatchers []*Watcher
	scratch       []*Watcher
}

// entry is the stored state of one key. The key itself lives in the index.
type entry struct {
	value          []byte
	version        uint64
	createRevision uint64
	modRevision    uint64
}

// keyValue returns a copy of the entry's state that does not alias storage.
func (e *entry) keyValue(key []byte) KeyValue {
	return KeyValue{
		Key:            bytes.Clone(key),
		Value:          cloneValue(e.value),
		Version:        e.version,
		CreateRevision: e.createRevision,
		ModRevision:    e.modRevision,
	}
}

// NewStore returns an empty store at revision zero.
func NewStore(opts Options) *Store {
	if opts.MaxSessions <= 0 {
		opts.MaxSessions = DefaultMaxSessions
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Store{
		logger:      opts.Logger,
		maxSessions: opts.MaxSessions,
		index:       newIndex(),
		sessions:    newSessionTable(opts.MaxSessions),
		keyWatchers: make(map[string][]*Watcher),
	}
}

// newIndex returns an empty ordered index. The seed only shapes the skip
// list's towers, never its contents, so a per-index random seed keeps the
// structure unpredictable to clients that control key insertion order
// without affecting determinism of the state machine.
func newIndex() *ordered.Map[*entry] {
	return ordered.New[*entry](rand.Uint64()) //nolint:gosec // the seed shapes a data structure, it is not a secret
}

// Get returns the current state of key. The returned slices are copies.
func (s *Store) Get(key []byte) (KeyValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.index.Get(key)
	if !ok {
		return KeyValue{}, false
	}
	return e.keyValue(key), true
}

// Scan returns the keys in the half-open range [start, end) in ascending
// order, at most limit of them when limit is positive. An empty start begins
// at the smallest key and an empty end extends to the largest. The boolean
// reports whether the limit cut the result short. The returned slices are
// copies.
func (s *Store) Scan(start, end []byte, limit int) ([]KeyValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []KeyValue
	if limit > 0 {
		out = make([]KeyValue, 0, min(limit, scanPreallocLimit))
	}
	more := false
	s.index.Ascend(start, end, func(key []byte, e *entry) bool {
		if limit > 0 && len(out) == limit {
			more = true
			return false
		}
		out = append(out, e.keyValue(key))
		return true
	})
	return out, more
}

// scanPreallocLimit caps the capacity Scan reserves up front, so that a
// request with a huge limit over a small range does not allocate for keys
// that do not exist.
const scanPreallocLimit = 256

// Revision returns the revision of the last command handed to Apply, or the
// revision carried by the last restored snapshot, whichever is more recent.
func (s *Store) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len()
}

// Sessions returns the number of client sessions in the exactly-once table.
func (s *Store) Sessions() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions.len()
}

// Watchers returns the number of active watchers.
func (s *Store) Watchers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.rangeWatchers)
	for _, ws := range s.keyWatchers {
		n += len(ws)
	}
	return n
}
