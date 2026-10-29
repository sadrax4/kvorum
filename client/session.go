package client

import (
	"github.com/sadrax4/kvorum/api/kvorumpb"
)

// session is one exactly-once write stream. The server applies the commands
// of a session in sequence order and treats a repeated sequence as a retry,
// which only works while a session has at most one request in flight. The
// client therefore keeps a pool of sessions: a logical write borrows one for
// its whole duration, including retries, and returns it afterwards, so
// concurrent writes never share a sequence space.
type session struct {
	id  uint64
	seq uint64
}

// next allocates the sequence for a new logical write on this session.
func (s *session) next() *kvorumpb.Session {
	s.seq++
	return &kvorumpb.Session{ClientId: s.id, Sequence: s.seq}
}

// acquireSession borrows an idle session or creates one. The first session
// uses the configured ClientID; additional sessions, needed only when writes
// overlap, get random ids.
func (c *Client) acquireSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.idleSessions); n > 0 {
		s := c.idleSessions[n-1]
		c.idleSessions = c.idleSessions[:n-1]
		return s
	}
	id := c.cfg.ClientID
	if c.sessionsCreated > 0 {
		if random, err := randomClientID(); err == nil {
			id = random
		} else {
			// Derive a distinct id deterministically if the random source
			// fails; collisions with other clients remain astronomically
			// unlikely because the base id is random.
			id = c.cfg.ClientID + c.sessionsCreated
		}
	}
	c.sessionsCreated++
	return &session{id: id}
}

// releaseSession returns a session to the pool once its write has finished,
// successfully or not: the sequence it used may have been applied.
func (c *Client) releaseSession(s *session) {
	c.mu.Lock()
	c.idleSessions = append(c.idleSessions, s)
	c.mu.Unlock()
}
