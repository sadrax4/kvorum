package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()
	base := func() Config {
		return Config{LocalID: 1, ListenAddress: "127.0.0.1:0", Handler: &recordingHandler{}}
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "zero id", mutate: func(c *Config) { c.LocalID = raft.None }},
		{name: "empty address", mutate: func(c *Config) { c.ListenAddress = "" }},
		{name: "nil handler", mutate: func(c *Config) { c.Handler = nil }},
		{name: "negative dial timeout", mutate: func(c *Config) { c.DialTimeout = -time.Second }},
		{name: "negative send timeout", mutate: func(c *Config) { c.SendTimeout = -time.Second }},
		{name: "negative chunk", mutate: func(c *Config) { c.SnapshotChunkSize = -1 }},
		{name: "negative queue", mutate: func(c *Config) { c.QueueSize = -1 }},
		{name: "negative max message", mutate: func(c *Config) { c.MaxMessageSize = -1 }},
		{name: "negative max snapshot", mutate: func(c *Config) { c.MaxSnapshotSize = -1 }},
		{name: "chunk larger than message", mutate: func(c *Config) {
			c.MaxMessageSize = 1 << 10
			c.SnapshotChunkSize = 1 << 10
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := base()
			tc.mutate(&cfg)
			tr, err := New(cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v, want ErrInvalidConfig", err)
			}
			if tr != nil {
				t.Fatal("New returned a transport alongside an error")
			}
		})
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()
	tr, err := New(Config{LocalID: 1, ListenAddress: "127.0.0.1:0", Handler: &recordingHandler{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(tr.Stop)
	cfg := tr.cfg
	if cfg.DialTimeout != DefaultDialTimeout || cfg.SendTimeout != DefaultSendTimeout {
		t.Fatalf("timeouts = %v/%v", cfg.DialTimeout, cfg.SendTimeout)
	}
	if cfg.SnapshotChunkSize != DefaultSnapshotChunkSize || cfg.QueueSize != DefaultQueueSize {
		t.Fatalf("chunk/queue = %d/%d", cfg.SnapshotChunkSize, cfg.QueueSize)
	}
	if cfg.MaxMessageSize != DefaultMaxMessageSize || cfg.MaxSnapshotSize != DefaultMaxSnapshotSize {
		t.Fatalf("max sizes = %d/%d", cfg.MaxMessageSize, cfg.MaxSnapshotSize)
	}
	if cfg.Logger == nil {
		t.Fatal("Logger not defaulted")
	}
	if _, ok := cfg.Metrics.(NopMetrics); !ok {
		t.Fatalf("Metrics = %T, want NopMetrics", cfg.Metrics)
	}
}

func TestLimiter(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(0, 0)}
	l := newLimiter(clock.Now)
	if !l.allow(1) || l.allow(1) {
		t.Fatal("first call must pass, second within the interval must not")
	}
	if !l.allow(2) {
		t.Fatal("a different key is independent")
	}
	clock.Advance(999 * time.Millisecond)
	if l.allow(1) {
		t.Fatal("allowed before the interval elapsed")
	}
	clock.Advance(time.Millisecond)
	if !l.allow(1) {
		t.Fatal("denied after the interval elapsed")
	}
	l.forget(1)
	if !l.allow(1) {
		t.Fatal("forgotten key must pass immediately")
	}
}

func TestLimiterBoundsTrackedKeys(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(0, 0)}
	l := newLimiter(clock.Now)
	for i := range limiterMaxKeys {
		l.allow(raft.NodeID(i + 1))
	}
	// The table is full and nothing has expired: an extra key is admitted
	// but not recorded, so it is admitted again.
	extra := raft.NodeID(limiterMaxKeys + 1)
	if !l.allow(extra) {
		t.Fatal("untracked key must be admitted")
	}
	if !l.allow(extra) {
		t.Fatal("untracked key must be admitted again: it was not recorded")
	}
	if n := len(l.last); n != limiterMaxKeys {
		t.Fatalf("tracked keys = %d, want %d", n, limiterMaxKeys)
	}
	clock.Advance(time.Second)
	if !l.allow(extra) || l.allow(extra) {
		t.Fatal("after eviction the extra key must be tracked again")
	}
	if n := len(l.last); n != 1 {
		t.Fatalf("tracked keys after eviction = %d, want 1", n)
	}
}
