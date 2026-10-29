package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/internal/telemetry"
)

// Config configures a Server.
type Config struct {
	// ListenClient is the gRPC address for the KV and Admin services.
	ListenClient string
	// ListenHTTP is the address for metrics, health, and pprof. Empty
	// disables the HTTP listener.
	ListenHTTP string

	Node     *node.Node
	Metrics  *telemetry.Metrics
	Gatherer prometheus.Gatherer
	Logger   *slog.Logger

	// MaxScanLimit caps the number of keys returned by one Scan.
	MaxScanLimit int
	// WatchBuffer is the per-watcher event buffer; a slower client is
	// disconnected once it overflows.
	WatchBuffer int
	// DefaultRequestTimeout bounds requests that arrive without a deadline.
	DefaultRequestTimeout time.Duration
	// MaxMessageSize bounds inbound gRPC messages.
	MaxMessageSize int
}

func (c *Config) withDefaults() (Config, error) {
	cfg := *c
	if cfg.ListenClient == "" {
		return cfg, errors.New("server: ListenClient is required")
	}
	if cfg.Node == nil || cfg.Metrics == nil {
		return cfg, errors.New("server: Node and Metrics are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.MaxScanLimit <= 0 {
		cfg.MaxScanLimit = 10000
	}
	if cfg.WatchBuffer <= 0 {
		cfg.WatchBuffer = 1024
	}
	if cfg.DefaultRequestTimeout <= 0 {
		cfg.DefaultRequestTimeout = 10 * time.Second
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = 8 << 20
	}
	return cfg, nil
}

// Server hosts the gRPC and HTTP endpoints of a node.
type Server struct {
	cfg     Config
	node    *node.Node
	metrics *telemetry.Metrics
	logger  *slog.Logger

	grpcServer *grpc.Server
	httpServer *http.Server
	grpcLis    net.Listener
	httpLis    net.Listener

	wg       sync.WaitGroup
	stopOnce sync.Once
	// stopC is closed at the start of Stop so that long-lived streams such
	// as Watch end promptly instead of holding up the graceful drain.
	stopC chan struct{}
}

// New creates a server for the given node. Nothing is bound until Start.
func New(c Config) (*Server, error) {
	cfg, err := c.withDefaults()
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, node: cfg.Node, metrics: cfg.Metrics, logger: cfg.Logger.With("component", "server"), stopC: make(chan struct{})}

	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.ChainUnaryInterceptor(s.recoveryUnary, s.metricsUnary, s.deadlineUnary),
		grpc.ChainStreamInterceptor(s.recoveryStream, s.metricsStream),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	kvorumpb.RegisterKVServiceServer(s.grpcServer, &kvService{s: s})
	kvorumpb.RegisterAdminServiceServer(s.grpcServer, &adminService{s: s})

	if cfg.ListenHTTP != "" {
		mux := http.NewServeMux()
		if cfg.Gatherer != nil {
			mux.Handle("/metrics", promhttp.HandlerFor(cfg.Gatherer, promhttp.HandlerOpts{}))
		}
		mux.HandleFunc("/healthz", s.handleHealthz)
		mux.HandleFunc("/readyz", s.handleReadyz)
		mux.HandleFunc("/status", s.handleStatus)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		s.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	}
	return s, nil
}

// Start binds the listeners and serves in the background.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.ListenClient)
	if err != nil {
		return fmt.Errorf("server: listen client %s: %w", s.cfg.ListenClient, err)
	}
	s.grpcLis = lis
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.logger.Error("grpc server stopped", "error", err)
		}
	}()

	if s.httpServer != nil {
		hlis, err := net.Listen("tcp", s.cfg.ListenHTTP)
		if err != nil {
			s.grpcServer.Stop()
			return fmt.Errorf("server: listen http %s: %w", s.cfg.ListenHTTP, err)
		}
		s.httpLis = hlis
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.httpServer.Serve(hlis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("http server stopped", "error", err)
			}
		}()
	}
	s.logger.Info("server started", "client_address", s.ClientAddress(), "http_address", s.HTTPAddress())
	return nil
}

// Stop drains in-flight requests until ctx expires, then forces shutdown.
func (s *Server) Stop(ctx context.Context) error {
	var err error
	s.stopOnce.Do(func() {
		close(s.stopC)
		done := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.grpcServer.Stop()
			err = ctx.Err()
		}
		if s.httpServer != nil {
			if herr := s.httpServer.Shutdown(ctx); herr != nil && err == nil {
				err = herr
			}
		}
		s.wg.Wait()
	})
	return err
}

// ClientAddress returns the bound gRPC address.
func (s *Server) ClientAddress() string {
	if s.grpcLis == nil {
		return ""
	}
	return s.grpcLis.Addr().String()
}

// HTTPAddress returns the bound HTTP address, or empty if disabled.
func (s *Server) HTTPAddress() string {
	if s.httpLis == nil {
		return ""
	}
	return s.httpLis.Addr().String()
}

// handleHealthz reports liveness: the process serves and the node's run
// loop has not died.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if err := s.node.Err(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "node failed: %v\n", err)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz reports readiness: the node must be running and know a
// leader.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if !s.node.Running() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("node stopped\n"))
		return
	}
	if _, ok := s.node.Leader(); !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no leader\n"))
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{
		"id":             st.ID,
		"state":          st.State.String(),
		"term":           st.Term,
		"leader":         st.Leader,
		"commit_index":   st.Commit,
		"applied_index":  st.Applied,
		"last_index":     st.LastIndex,
		"snapshot_index": st.SnapshotIndex,
		"members":        st.Members,
		"version":        telemetry.Version,
	})
}

func methodName(full string) string {
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[i+1:]
	}
	return full
}

func (s *Server) metricsUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	op := methodName(info.FullMethod)
	s.metrics.KVRequests.WithLabelValues(op, status.Code(err).String()).Inc()
	s.metrics.KVRequestDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return resp, err
}

func (s *Server) metricsStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	op := methodName(info.FullMethod)
	s.metrics.KVRequests.WithLabelValues(op, status.Code(err).String()).Inc()
	s.metrics.KVRequestDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return err
}

func (s *Server) deadlineUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, cancel := requestDeadline(ctx, s.cfg.DefaultRequestTimeout)
	defer cancel()
	return handler(ctx, req)
}

func (s *Server) recoveryUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

func (s *Server) recoveryStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in stream handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(srv, ss)
}
