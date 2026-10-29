package kv

import (
	"cmp"
	"container/list"
	"fmt"
	"slices"
)

// session is the exactly-once record of one client.
type session struct {
	clientID uint64
	// sequence is the last sequence number applied for the client.
	sequence uint64
	// revision is the revision of the last command that touched the session,
	// whether it was applied or recognized as a duplicate. Eviction removes
	// the session with the lowest revision.
	revision uint64
	// result is the deep-copied outcome of the command with sequence.
	result Result
	elem   *list.Element
}

// sessionTable is a bounded map from client id to session with
// least-recently-used eviction. Because every Apply carries a revision
// greater than the previous one, recency order and revision order coincide,
// so a move-to-front list is enough to find the eviction victim in O(1).
type sessionTable struct {
	limit int
	byID  map[uint64]*session
	// lru holds *session values, most recently used at the front.
	lru list.List
}

func newSessionTable(limit int) *sessionTable {
	return &sessionTable{limit: limit, byID: make(map[uint64]*session)}
}

func (t *sessionTable) len() int {
	return len(t.byID)
}

// lookup classifies a command against the client's session and marks the
// session as used at revision. When handled is true the command must not be
// executed and res is the result to return: the recorded result flagged as a
// duplicate, or ErrStaleSequence. Otherwise the command is new and must be
// executed and recorded.
func (t *sessionTable) lookup(clientID, sequence, revision uint64) (res Result, handled bool) {
	s := t.byID[clientID]
	if s == nil {
		return Result{}, false
	}
	s.revision = revision
	t.lru.MoveToFront(s.elem)
	switch {
	case sequence == s.sequence:
		res = s.result.clone()
		res.Duplicate = true
		return res, true
	case sequence < s.sequence:
		return Result{Err: fmt.Errorf("%w: client %d sent sequence %d after %d",
			ErrStaleSequence, clientID, sequence, s.sequence)}, true
	default:
		return Result{}, false
	}
}

// record stores the outcome of the client's command with sequence, creating
// the session if needed. When the table exceeds its limit the least recently
// used session is evicted and its client id returned.
func (t *sessionTable) record(clientID, sequence, revision uint64, res Result) (evicted uint64, ok bool) {
	s := t.byID[clientID]
	if s == nil {
		s = &session{clientID: clientID}
		s.elem = t.lru.PushFront(s)
		t.byID[clientID] = s
	} else {
		t.lru.MoveToFront(s.elem)
	}
	s.sequence = sequence
	s.revision = revision
	s.result = res.clone()
	if t.lru.Len() > t.limit {
		return t.evictOldest(), true
	}
	return 0, false
}

// evictOldest removes the least recently used session and returns its client
// id. The table must not be empty.
func (t *sessionTable) evictOldest() uint64 {
	back := t.lru.Back()
	s := back.Value.(*session)
	t.lru.Remove(back)
	delete(t.byID, s.clientID)
	return s.clientID
}

// sorted returns the sessions ordered by client id, the order snapshots use.
func (t *sessionTable) sorted() []*session {
	out := make([]*session, 0, len(t.byID))
	for _, s := range t.byID {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b *session) int { return cmp.Compare(a.clientID, b.clientID) })
	return out
}

// restore replaces the table contents with sessions decoded from a snapshot.
// Recency is rebuilt from the recorded revisions; if there are more sessions
// than the limit, the least recently used are dropped.
func (t *sessionTable) restore(sessions []*session) {
	slices.SortFunc(sessions, func(a, b *session) int {
		return cmp.Or(cmp.Compare(a.revision, b.revision), cmp.Compare(a.clientID, b.clientID))
	})
	t.lru.Init()
	clear(t.byID)
	for _, s := range sessions {
		s.elem = t.lru.PushFront(s)
		t.byID[s.clientID] = s
	}
	for t.lru.Len() > t.limit {
		t.evictOldest()
	}
}
