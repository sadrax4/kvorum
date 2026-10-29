package transport

import (
	"sync"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

// rateLimitInterval is the minimum spacing between repeated events for one
// key: unreachable reports to the handler and drop diagnostics in the log.
const rateLimitInterval = time.Second

// limiterMaxKeys bounds the number of node ids a limiter tracks so that a
// stream of messages to arbitrary unknown ids cannot grow memory without
// limit. Beyond the bound, untracked keys are admitted without being recorded.
const limiterMaxKeys = 1024

// limiter admits at most one event per key per rateLimitInterval. It is used
// to throttle unreachable reports and diagnostic logs per peer.
type limiter struct {
	now func() time.Time

	mu   sync.Mutex
	last map[raft.NodeID]time.Time
}

func newLimiter(now func() time.Time) *limiter {
	return &limiter{
		now:  now,
		last: make(map[raft.NodeID]time.Time),
	}
}

// allow reports whether an event for key may proceed now and, if so, records
// it.
func (l *limiter) allow(key raft.NodeID) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if last, ok := l.last[key]; ok {
		if now.Sub(last) < rateLimitInterval {
			return false
		}
		l.last[key] = now
		return true
	}
	if len(l.last) >= limiterMaxKeys {
		l.evictExpired(now)
	}
	if len(l.last) < limiterMaxKeys {
		l.last[key] = now
	}
	return true
}

// forget drops the record for key, for example when a peer is removed.
func (l *limiter) forget(key raft.NodeID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.last, key)
}

// evictExpired removes every record whose interval has elapsed. The caller
// holds mu.
func (l *limiter) evictExpired(now time.Time) {
	for key, last := range l.last {
		if now.Sub(last) >= rateLimitInterval {
			delete(l.last, key)
		}
	}
}
