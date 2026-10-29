// Package client is the Go client library for kvorum.
//
// A Client talks to the KVService and AdminService gRPC APIs of a kvorum
// cluster. It keeps one lazily dialled connection per endpoint, follows the
// LeaderHint details that non-leader nodes attach to their errors, rotates to
// another endpoint with exponential backoff when a node is unreachable, and
// tags every write with a Session so that retries are applied exactly once by
// the replicated state machine.
//
// A Client is safe for concurrent use. Close releases every connection and
// ends every Watch.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Default values applied by New for zero Config fields.
const (
	DefaultDialTimeout      = 3 * time.Second
	DefaultRequestTimeout   = 5 * time.Second
	DefaultMaxRetries       = 8
	DefaultRetryBaseBackoff = 50 * time.Millisecond
	DefaultRetryMaxBackoff  = 2 * time.Second
)

const (
	keepaliveTime    = time.Minute
	keepaliveTimeout = 20 * time.Second
)

// Config configures a Client.
type Config struct {
	// Endpoints lists the client addresses of cluster members. At least one
	// is required. Requests go to the first endpoint until a leader is
	// learnt, then to the last known leader.
	Endpoints []string
	// DialTimeout bounds a single connection attempt. Default 3s.
	DialTimeout time.Duration
	// RequestTimeout bounds a single attempt of a unary request. The caller's
	// context bounds the whole logical request including retries. Default 5s.
	RequestTimeout time.Duration
	// MaxRetries is the number of retries after the first attempt of a
	// logical request. Default 8.
	MaxRetries int
	// RetryBaseBackoff is the delay before the first retry; it doubles on
	// every further retry. Default 50ms.
	RetryBaseBackoff time.Duration
	// RetryMaxBackoff caps the delay between retries. Default 2s.
	RetryMaxBackoff time.Duration
	// ClientID identifies this client in write Sessions. It must be unique
	// across concurrently running clients; when zero New draws a random one.
	ClientID uint64
	// Logger receives debug and warning messages. Nil discards them.
	Logger *slog.Logger
}

func (cfg Config) withDefaults() Config {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	} else if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.RetryBaseBackoff <= 0 {
		cfg.RetryBaseBackoff = DefaultRetryBaseBackoff
	}
	if cfg.RetryMaxBackoff <= 0 {
		cfg.RetryMaxBackoff = DefaultRetryMaxBackoff
	}
	if cfg.RetryMaxBackoff < cfg.RetryBaseBackoff {
		cfg.RetryMaxBackoff = cfg.RetryBaseBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return cfg
}

// Client is a kvorum client. Create one with New and release it with Close.
type Client struct {
	cfg       Config
	log       *slog.Logger
	endpoints *endpointRing

	closeCtx    context.Context
	closeCancel context.CancelFunc
	watchers    sync.WaitGroup

	mu              sync.Mutex
	conns           map[string]*grpc.ClientConn
	closed          bool
	idleSessions    []*session
	sessionsCreated uint64
}

// New creates a Client. It validates the configuration and applies defaults
// but opens no connection: endpoints are dialled on first use.
func New(cfg Config) (*Client, error) {
	endpoints := normaliseEndpoints(cfg.Endpoints)
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	cfg = cfg.withDefaults()
	if cfg.ClientID == 0 {
		id, err := randomClientID()
		if err != nil {
			return nil, err
		}
		cfg.ClientID = id
	}
	closeCtx, closeCancel := context.WithCancel(context.Background())
	return &Client{
		cfg:         cfg,
		log:         cfg.Logger.With("component", "kvorum-client"),
		endpoints:   newEndpointRing(endpoints),
		closeCtx:    closeCtx,
		closeCancel: closeCancel,
		conns:       make(map[string]*grpc.ClientConn, len(endpoints)),
	}, nil
}

// ClientID returns the id used in the Sessions of this client's writes.
func (c *Client) ClientID() uint64 {
	return c.cfg.ClientID
}

// Close ends every Watch, waits for their goroutines, and closes every
// connection. Calling it more than once is harmless; later calls return nil.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	c.closeCancel()
	c.watchers.Wait()

	var errs []error
	for addr, cc := range conns {
		if err := cc.Close(); err != nil {
			errs = append(errs, fmt.Errorf("client: close connection to %s: %w", addr, err))
		}
	}
	return errors.Join(errs...)
}

// conn returns the connection to addr, creating it when missing.
func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: c.cfg.DialTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("client: create connection to %s: %w", addr, err)
	}
	c.conns[addr] = cc
	return cc, nil
}

func normaliseEndpoints(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, e := range in {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if _, dup := seen[e]; dup {
			continue
		}
		seen[e] = struct{}{}
		out = append(out, e)
	}
	return out
}

func randomClientID() (uint64, error) {
	var buf [8]byte
	for {
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, fmt.Errorf("client: generate client id: %w", err)
		}
		if id := binary.LittleEndian.Uint64(buf[:]); id != 0 {
			return id, nil
		}
	}
}
