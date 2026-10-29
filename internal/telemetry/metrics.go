package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sadrax4/kvorum/raft"
)

// Metrics holds every Prometheus collector exported by a kvorum server. All
// metric names carry the kvorum_ prefix.
type Metrics struct {
	// Raft.
	RaftTerm             prometheus.Gauge
	RaftCommitIndex      prometheus.Gauge
	RaftAppliedIndex     prometheus.Gauge
	RaftLastIndex        prometheus.Gauge
	RaftSnapshotIndex    prometheus.Gauge
	RaftState            prometheus.Gauge
	RaftIsLeader         prometheus.Gauge
	RaftLeaderChanges    prometheus.Counter
	RaftConfChanges      prometheus.Counter
	RaftProposals        *prometheus.CounterVec
	RaftMessagesSent     *prometheus.CounterVec
	RaftMessagesReceived *prometheus.CounterVec
	RaftReadyDuration    prometheus.Histogram
	RaftApplyDuration    prometheus.Histogram
	RaftSnapshots        *prometheus.CounterVec

	// Write-ahead log.
	WALSyncDuration prometheus.Histogram
	WALBytesWritten prometheus.Counter
	WALSegments     prometheus.Gauge

	// Key-value API.
	KVKeys            prometheus.Gauge
	KVRequests        *prometheus.CounterVec
	KVRequestDuration *prometheus.HistogramVec
	KVWatchers        prometheus.Gauge

	// Transport.
	TransportMessagesSent *prometheus.CounterVec
	TransportBytesSent    prometheus.Counter
	TransportSendFailures *prometheus.CounterVec
	TransportDropped      *prometheus.CounterVec
	TransportSnapshots    *prometheus.CounterVec

	// Build information, always 1.
	BuildInfo *prometheus.GaugeVec
}

// NewMetrics creates the collectors and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		panic("telemetry: registerer must not be nil")
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "kvorum", Name: name, Help: help})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kvorum", Name: name, Help: help})
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "kvorum", Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, buckets []float64) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "kvorum", Name: name, Help: help, Buckets: buckets})
	}
	latency := prometheus.ExponentialBuckets(0.0001, 2, 16) // 100µs .. ~3.3s

	m := &Metrics{
		RaftTerm:             gauge("raft_term", "Current Raft term."),
		RaftCommitIndex:      gauge("raft_commit_index", "Highest log index known to be committed."),
		RaftAppliedIndex:     gauge("raft_applied_index", "Highest log index applied to the state machine."),
		RaftLastIndex:        gauge("raft_last_index", "Last index in the local log."),
		RaftSnapshotIndex:    gauge("raft_snapshot_index", "Index of the latest local snapshot."),
		RaftState:            gauge("raft_state", "Raft role: 0 follower, 1 pre-candidate, 2 candidate, 3 leader."),
		RaftIsLeader:         gauge("raft_is_leader", "1 when this node is the leader."),
		RaftLeaderChanges:    counter("raft_leader_changes_total", "Number of observed leader changes."),
		RaftConfChanges:      counter("raft_conf_changes_total", "Number of applied membership changes."),
		RaftProposals:        counterVec("raft_proposals_total", "Proposals by outcome.", "result"),
		RaftMessagesSent:     counterVec("raft_messages_sent_total", "Raft messages sent by type.", "type"),
		RaftMessagesReceived: counterVec("raft_messages_received_total", "Raft messages received by type.", "type"),
		RaftReadyDuration:    histogram("raft_ready_duration_seconds", "Time spent processing one Ready.", latency),
		RaftApplyDuration:    histogram("raft_apply_duration_seconds", "Time spent applying a batch of committed entries.", latency),
		RaftSnapshots:        counterVec("raft_snapshots_total", "Snapshots by direction.", "direction"),

		WALSyncDuration: histogram("wal_sync_duration_seconds", "Duration of write-ahead log fsync calls.", latency),
		WALBytesWritten: counter("wal_bytes_written_total", "Bytes appended to the write-ahead log."),
		WALSegments:     gauge("wal_segments", "Number of write-ahead log segment files."),

		KVKeys:     gauge("kv_keys", "Number of keys in the store."),
		KVRequests: counterVec("kv_requests_total", "Client requests by operation and status.", "op", "status"),
		KVRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kvorum", Name: "kv_request_duration_seconds", Help: "Client request latency by operation.", Buckets: latency,
		}, []string{"op"}),
		KVWatchers: gauge("kv_watchers", "Number of active watch streams."),

		TransportMessagesSent: counterVec("transport_messages_sent_total", "Messages handed to the transport by type.", "type"),
		TransportBytesSent:    counter("transport_bytes_sent_total", "Bytes sent to peers."),
		TransportSendFailures: counterVec("transport_send_failures_total", "Failed sends by peer.", "peer"),
		TransportDropped:      counterVec("transport_dropped_messages_total", "Messages dropped because a peer queue was full or the peer is unknown.", "peer"),
		TransportSnapshots:    counterVec("transport_snapshots_total", "Snapshot transfers by result.", "result"),

		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "kvorum", Name: "build_info", Help: "Build information; the value is always 1.",
		}, []string{"version", "commit", "go_version"}),
	}
	m.BuildInfo.WithLabelValues(Version, Commit, GoVersion()).Set(1)

	reg.MustRegister(
		m.RaftTerm, m.RaftCommitIndex, m.RaftAppliedIndex, m.RaftLastIndex, m.RaftSnapshotIndex,
		m.RaftState, m.RaftIsLeader, m.RaftLeaderChanges, m.RaftConfChanges, m.RaftProposals,
		m.RaftMessagesSent, m.RaftMessagesReceived, m.RaftReadyDuration, m.RaftApplyDuration, m.RaftSnapshots,
		m.WALSyncDuration, m.WALBytesWritten, m.WALSegments,
		m.KVKeys, m.KVRequests, m.KVRequestDuration, m.KVWatchers,
		m.TransportMessagesSent, m.TransportBytesSent, m.TransportSendFailures, m.TransportDropped, m.TransportSnapshots,
		m.BuildInfo,
	)
	return m
}

// SetRaftState records the node's role.
func (m *Metrics) SetRaftState(state raft.StateType) {
	m.RaftState.Set(float64(state))
	if state == raft.StateLeader {
		m.RaftIsLeader.Set(1)
	} else {
		m.RaftIsLeader.Set(0)
	}
}
