package kv

import (
	"bytes"
	"strconv"
	"sync"
)

// EventType is the kind of change an Event describes.
type EventType uint8

const (
	// EventPut reports that a key was created or overwritten.
	EventPut EventType = 1
	// EventDelete reports that a key was removed.
	EventDelete EventType = 2
)

// String implements fmt.Stringer.
func (t EventType) String() string {
	switch t {
	case EventPut:
		return "put"
	case EventDelete:
		return "delete"
	default:
		return "unknown(" + strconv.Itoa(int(t)) + ")"
	}
}

// Event describes one change to a key. Events delivered to several watchers
// share their slices, so consumers must treat them as read-only.
type Event struct {
	Type EventType
	// KV is the key after the change. For EventDelete only Key and
	// ModRevision are set.
	KV KeyValue
	// PrevKV is the key's state before the change, or nil if it did not exist.
	PrevKV *KeyValue
	// Revision is the revision of the command that produced the event.
	Revision uint64
}

// Watcher delivers the events of a key or key range. Watchers are created by
// Store.Watch; the zero value is not usable.
type Watcher struct {
	store *Store
	start []byte
	end   []byte
	// single selects exactly the key equal to start; unbounded extends the
	// range from start to the largest key.
	single    bool
	unbounded bool
	events    chan Event

	// registered and index are guarded by store.mu. index is the watcher's
	// position in store.rangeWatchers or in store.keyWatchers[string(start)].
	registered bool
	index      int

	mu  sync.Mutex
	err error
}

// Watch registers a watcher for the events of a key or key range starting
// from the next command applied. An empty end watches exactly key == start;
// an end consisting of the single byte 0x00 watches every key not smaller
// than start; any other end watches the half-open range [start, end).
//
// buffer is the capacity of the event channel; values that are not positive
// select DefaultWatchBuffer. When an event does not fit, the watcher is
// cancelled with ErrWatcherOverflow and must be re-established after
// re-reading the keys of interest.
func (s *Store) Watch(start, end []byte, buffer int) *Watcher {
	if buffer <= 0 {
		buffer = DefaultWatchBuffer
	}
	w := &Watcher{
		store:     s,
		start:     bytes.Clone(start),
		end:       bytes.Clone(end),
		single:    len(end) == 0,
		unbounded: len(end) == 1 && end[0] == 0,
		events:    make(chan Event, buffer),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w.registered = true
	if w.single {
		k := string(w.start)
		w.index = len(s.keyWatchers[k])
		s.keyWatchers[k] = append(s.keyWatchers[k], w)
	} else {
		w.index = len(s.rangeWatchers)
		s.rangeWatchers = append(s.rangeWatchers, w)
	}
	return w
}

// Events returns the channel on which events are delivered in revision order.
// The channel is closed when the watcher is cancelled, after any events that
// were already buffered; Err then explains why.
func (w *Watcher) Events() <-chan Event {
	return w.events
}

// Cancel stops the watcher and closes its channel. It is safe to call more
// than once and after the store has already cancelled the watcher.
func (w *Watcher) Cancel() {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.cancelWatcherLocked(w, ErrWatcherCancelled)
}

// Err returns nil while the watcher is active, ErrWatcherCancelled after
// Cancel, and ErrWatcherOverflow after the store cancelled the watcher
// because it could not deliver an event.
func (w *Watcher) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// coversRange reports whether a range watcher's range contains key.
func (w *Watcher) coversRange(key []byte) bool {
	if bytes.Compare(key, w.start) < 0 {
		return false
	}
	return w.unbounded || bytes.Compare(key, w.end) < 0
}

// notify builds the event for a mutation of key and delivers it to every
// matching watcher. cur is the entry after the change (nil for a delete) and
// prev the state before it (nil when the key did not exist); prev's value
// slice must no longer be referenced by the index. The store's write lock
// must be held.
func (s *Store) notify(revision uint64, typ EventType, key []byte, cur *entry, prev *KeyValue) {
	s.scratch = append(s.scratch[:0], s.keyWatchers[string(key)]...)
	for _, w := range s.rangeWatchers {
		if w.coversRange(key) {
			s.scratch = append(s.scratch, w)
		}
	}
	if len(s.scratch) == 0 {
		return
	}

	ev := Event{Type: typ, Revision: revision, KV: KeyValue{Key: bytes.Clone(key), ModRevision: revision}}
	if cur != nil {
		ev.KV.Value = cloneValue(cur.value)
		ev.KV.Version = cur.version
		ev.KV.CreateRevision = cur.createRevision
	}
	if prev != nil {
		p := *prev
		p.Key = ev.KV.Key
		ev.PrevKV = &p
	}
	for _, w := range s.scratch {
		select {
		case w.events <- ev:
		default:
			s.cancelWatcherLocked(w, ErrWatcherOverflow)
			s.logger.Warn("cancelled watcher that fell behind",
				"revision", revision, "buffer", cap(w.events))
		}
	}
	clear(s.scratch)
}

// cancelWatcherLocked unregisters w, records err, and closes its channel. It
// is a no-op for a watcher that is already cancelled. The store's write lock
// must be held.
func (s *Store) cancelWatcherLocked(w *Watcher, err error) {
	if !w.registered {
		return
	}
	w.registered = false
	if w.single {
		k := string(w.start)
		if rest, empty := removeWatcher(s.keyWatchers[k], w.index); empty {
			delete(s.keyWatchers, k)
		} else {
			s.keyWatchers[k] = rest
		}
	} else {
		s.rangeWatchers, _ = removeWatcher(s.rangeWatchers, w.index)
	}
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
	close(w.events)
}

// removeWatcher deletes position i from ws by moving the last watcher into
// its place, and reports whether the slice is now empty.
func removeWatcher(ws []*Watcher, i int) (rest []*Watcher, empty bool) {
	last := len(ws) - 1
	if i != last {
		ws[i] = ws[last]
		ws[i].index = i
	}
	ws[last] = nil
	ws = ws[:last]
	return ws, len(ws) == 0
}

// cancelAllWatchersLocked cancels every watcher with err. The store's write
// lock must be held.
func (s *Store) cancelAllWatchersLocked(err error) {
	all := make([]*Watcher, 0, len(s.rangeWatchers))
	all = append(all, s.rangeWatchers...)
	for _, ws := range s.keyWatchers {
		all = append(all, ws...)
	}
	for _, w := range all {
		s.cancelWatcherLocked(w, err)
	}
}
