// Command kvorum runs a kvorum server: one member of a replicated key-value
// cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/sadrax4/kvorum/internal/node"
	"github.com/sadrax4/kvorum/internal/server"
	"github.com/sadrax4/kvorum/internal/telemetry"
	"github.com/sadrax4/kvorum/raft"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "kvorum:", err)
		os.Exit(1)
	}
}

// options holds every command-line flag. Each flag can also be provided
// through an environment variable named KVORUM_<FLAG> with dashes replaced by
// underscores; the flag wins when both are set.
type options struct {
	id                uint64
	dataDir           string
	listenClient      string
	listenRaft        string
	listenHTTP        string
	advertiseClient   string
	advertiseRaft     string
	initialCluster    string
	join              bool
	snapshotThreshold uint64
	tickInterval      time.Duration
	electionTick      int
	heartbeatTick     int
	logLevel          string
	logFormat         string
	showVersion       bool
}

func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("kvorum", flag.ContinueOnError)
	fs.Uint64Var(&o.id, "id", envUint("KVORUM_ID", 0), "node id (required, non-zero)")
	fs.StringVar(&o.dataDir, "data-dir", envString("KVORUM_DATA_DIR", ""), "data directory (required)")
	fs.StringVar(&o.listenClient, "listen-client", envString("KVORUM_LISTEN_CLIENT", "0.0.0.0:2379"), "KV/Admin gRPC listen address")
	fs.StringVar(&o.listenRaft, "listen-raft", envString("KVORUM_LISTEN_RAFT", "0.0.0.0:2380"), "Raft gRPC listen address")
	fs.StringVar(&o.listenHTTP, "listen-http", envString("KVORUM_LISTEN_HTTP", "0.0.0.0:2381"), "metrics/health/pprof listen address (empty disables)")
	fs.StringVar(&o.advertiseClient, "advertise-client", envString("KVORUM_ADVERTISE_CLIENT", ""), "client address published to other nodes (defaults to --listen-client)")
	fs.StringVar(&o.advertiseRaft, "advertise-raft", envString("KVORUM_ADVERTISE_RAFT", ""), "raft address peers dial (defaults to --listen-raft)")
	fs.StringVar(&o.initialCluster, "initial-cluster", envString("KVORUM_INITIAL_CLUSTER", ""), "id=raftaddr,... bootstrap membership, or the existing members with --join")
	fs.BoolVar(&o.join, "join", envBool("KVORUM_JOIN", false), "start empty and wait to be added by the leader")
	fs.Uint64Var(&o.snapshotThreshold, "snapshot-threshold", envUint("KVORUM_SNAPSHOT_THRESHOLD", 10000), "applied entries between snapshots")
	fs.DurationVar(&o.tickInterval, "tick-interval", envDuration("KVORUM_TICK_INTERVAL", 100*time.Millisecond), "logical tick length")
	fs.IntVar(&o.electionTick, "election-tick", envInt("KVORUM_ELECTION_TICK", 10), "ticks without a leader before an election")
	fs.IntVar(&o.heartbeatTick, "heartbeat-tick", envInt("KVORUM_HEARTBEAT_TICK", 1), "ticks between heartbeats")
	fs.StringVar(&o.logLevel, "log-level", envString("KVORUM_LOG_LEVEL", "info"), "debug|info|warn|error")
	fs.StringVar(&o.logFormat, "log-format", envString("KVORUM_LOG_FORMAT", "json"), "json|text")
	fs.BoolVar(&o.showVersion, "version", false, "print version and exit")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: kvorum [flags]\n\nFlags (each also readable from KVORUM_<FLAG> environment variables):\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 {
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return o, nil
}

func run(args []string) error {
	o, err := parseFlags(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if o.showVersion {
		fmt.Printf("kvorum %s (commit %s, built %s, %s)\n", telemetry.Version, telemetry.Commit, telemetry.BuildDate, telemetry.GoVersion())
		return nil
	}
	if o.id == 0 {
		return errors.New("--id is required")
	}
	if o.dataDir == "" {
		return errors.New("--data-dir is required")
	}
	initial, err := parseInitialCluster(o.initialCluster)
	if err != nil {
		return err
	}
	if o.advertiseClient == "" {
		o.advertiseClient = o.listenClient
	}
	if o.advertiseRaft == "" {
		o.advertiseRaft = o.listenRaft
	}

	logger, err := telemetry.NewLogger(o.logLevel, o.logFormat, os.Stderr)
	if err != nil {
		return err
	}
	logger.Info("starting kvorum", "version", telemetry.Version, "commit", telemetry.Commit, "id", o.id, "data_dir", o.dataDir)

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := telemetry.NewMetrics(registry)

	n, err := node.Open(node.Config{
		ID:                raft.NodeID(o.id),
		DataDir:           o.dataDir,
		ListenRaft:        o.listenRaft,
		AdvertiseRaft:     o.advertiseRaft,
		AdvertiseClient:   o.advertiseClient,
		InitialCluster:    initial,
		Join:              o.join,
		TickInterval:      o.tickInterval,
		ElectionTick:      o.electionTick,
		HeartbeatTick:     o.heartbeatTick,
		SnapshotThreshold: o.snapshotThreshold,
		Logger:            logger,
		Metrics:           metrics,
	})
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}

	srv, err := server.New(server.Config{
		ListenClient: o.listenClient,
		ListenHTTP:   o.listenHTTP,
		Node:         n,
		Metrics:      metrics,
		Gatherer:     registry,
		Logger:       logger,
	})
	if err != nil {
		stopNode(n, logger)
		return err
	}
	if err := srv.Start(); err != nil {
		stopNode(n, logger)
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigC:
		logger.Info("received signal; shutting down", "signal", sig.String())
	case <-n.Removed():
		logger.Warn("node was removed from the cluster; shutting down")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		logger.Warn("server shutdown", "error", err)
	}
	if err := n.Stop(ctx); err != nil {
		logger.Warn("node shutdown", "error", err)
	}
	logger.Info("shutdown complete")
	return nil
}

func stopNode(n *node.Node, logger interface{ Warn(string, ...any) }) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		logger.Warn("node shutdown", "error", err)
	}
}

// parseInitialCluster parses "1=host:2380,2=host:2380" into a membership map.
func parseInitialCluster(s string) (map[raft.NodeID]string, error) {
	out := make(map[raft.NodeID]string)
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("--initial-cluster: entry %q must look like id=host:port", part)
		}
		n, err := strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("--initial-cluster: invalid id in %q", part)
		}
		if _, dup := out[raft.NodeID(n)]; dup {
			return nil, fmt.Errorf("--initial-cluster: duplicate id %d", n)
		}
		out[raft.NodeID(n)] = strings.TrimSpace(addr)
	}
	return out, nil
}

func envString(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envUint(name string, def uint64) uint64 {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envInt(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(name string, def bool) bool {
	if v, ok := os.LookupEnv(name); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(name); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
