package kv

import (
	"bytes"
	"fmt"
)

// Apply executes cmd at the given revision and returns its outcome. The
// revision must be strictly greater than the one of the previous call (or of
// the last restored snapshot); otherwise nothing happens and the result
// carries ErrRevisionNotMonotonic. Every other call advances Revision to
// revision, including calls whose command turns out to be invalid, a
// duplicate, or stale.
//
// Apply never blocks on watchers: an event that does not fit in a watcher's
// channel cancels that watcher instead. Input slices are copied before they
// are retained and output slices never alias storage.
func (s *Store) Apply(revision uint64, cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	if revision <= s.revision {
		return Result{Err: fmt.Errorf("%w: got %d after %d", ErrRevisionNotMonotonic, revision, s.revision)}
	}
	s.revision = revision

	if err := cmd.Validate(); err != nil {
		return Result{Err: err}
	}
	if cmd.ClientID != 0 {
		if res, handled := s.sessions.lookup(cmd.ClientID, cmd.Sequence, revision); handled {
			return res
		}
	}
	res := s.execute(revision, cmd)
	if cmd.ClientID != 0 {
		if evicted, ok := s.sessions.record(cmd.ClientID, cmd.Sequence, revision, res); ok {
			s.logger.Debug("evicted least recently used session",
				"client_id", evicted, "revision", revision, "max_sessions", s.maxSessions)
		}
	}
	return res
}

// execute runs a validated, non-duplicate command.
func (s *Store) execute(revision uint64, cmd Command) Result {
	switch cmd.Op {
	case OpPut:
		return s.put(revision, cmd, false)
	case OpDelete:
		return s.delete(revision, cmd)
	case OpCompareAndSwap:
		return s.compareAndSwap(revision, cmd)
	default:
		return Result{Err: fmt.Errorf("%w %d", ErrUnknownOp, cmd.Op)}
	}
}

// put writes cmd.Value under cmd.Key. succeeded is copied into the result so
// that compareAndSwap can share the implementation.
func (s *Store) put(revision uint64, cmd Command, succeeded bool) Result {
	e, existed := s.index.Get(cmd.Key)
	var prev *KeyValue
	if existed {
		// The previous value slice leaves the index here and is referenced by
		// nothing else, so the event may carry it without a copy.
		prev = &KeyValue{
			Value:          e.value,
			Version:        e.version,
			CreateRevision: e.createRevision,
			ModRevision:    e.modRevision,
		}
		e.value = cloneValue(cmd.Value)
		e.version++
		e.modRevision = revision
	} else {
		e = &entry{
			value:          cloneValue(cmd.Value),
			version:        1,
			createRevision: revision,
			modRevision:    revision,
		}
		s.index.Put(bytes.Clone(cmd.Key), e)
	}

	kv := e.keyValue(cmd.Key)
	res := Result{KV: &kv, Succeeded: succeeded}
	if cmd.PrevKV && prev != nil {
		res.PrevKV = &KeyValue{
			Key:            bytes.Clone(cmd.Key),
			Value:          cloneValue(prev.Value),
			Version:        prev.Version,
			CreateRevision: prev.CreateRevision,
			ModRevision:    prev.ModRevision,
		}
	}
	s.notify(revision, EventPut, cmd.Key, e, prev)
	return res
}

// delete removes cmd.Key if present.
func (s *Store) delete(revision uint64, cmd Command) Result {
	e, existed := s.index.Delete(cmd.Key)
	if !existed {
		return Result{}
	}
	prev := &KeyValue{
		Value:          e.value,
		Version:        e.version,
		CreateRevision: e.createRevision,
		ModRevision:    e.modRevision,
	}
	res := Result{Deleted: true}
	if cmd.PrevKV {
		res.PrevKV = &KeyValue{
			Key:            bytes.Clone(cmd.Key),
			Value:          cloneValue(prev.Value),
			Version:        prev.Version,
			CreateRevision: prev.CreateRevision,
			ModRevision:    prev.ModRevision,
		}
	}
	s.notify(revision, EventDelete, cmd.Key, nil, prev)
	return res
}

// compareAndSwap writes cmd.Value when the key's current version matches
// cmd.ExpectedVersion, treating an absent key as version zero. On a mismatch
// nothing changes and the result reports the key's current state.
func (s *Store) compareAndSwap(revision uint64, cmd Command) Result {
	e, existed := s.index.Get(cmd.Key)
	var current uint64
	if existed {
		current = e.version
	}
	if current != cmd.ExpectedVersion {
		res := Result{Succeeded: false}
		if existed {
			kv := e.keyValue(cmd.Key)
			res.KV = &kv
		}
		return res
	}
	return s.put(revision, cmd, true)
}
