package telemetry

import (
	"bytes"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sadrax4/kvorum/raft"
)

func TestNewMetricsRegisters(t *testing.T) {
	t.Parallel()
	for range 2 {
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)
		m.SetRaftState(raft.StateLeader)
		m.RaftProposals.WithLabelValues("committed").Inc()
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		names := make(map[string]bool, len(families))
		for _, f := range families {
			names[f.GetName()] = true
		}
		for _, want := range []string{"kvorum_build_info", "kvorum_raft_is_leader", "kvorum_raft_proposals_total", "kvorum_wal_sync_duration_seconds"} {
			if !names[want] {
				t.Errorf("metric %s not gathered", want)
			}
		}
	}
}

func TestNewMetricsRejectsNilRegisterer(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	NewMetrics(nil)
}

func TestNewLogger(t *testing.T) {
	t.Parallel()
	cases := []struct {
		level, format string
		wantErr       bool
		contains      string
	}{
		{"info", "json", false, `"msg":"hello"`},
		{"debug", "text", false, "msg=hello"},
		{"WARN", "json", false, ""},
		{"loud", "json", true, ""},
		{"info", "xml", true, ""},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		logger, err := NewLogger(tc.level, tc.format, &buf)
		if tc.wantErr {
			if err == nil {
				t.Errorf("level=%s format=%s: expected error", tc.level, tc.format)
			}
			continue
		}
		if err != nil {
			t.Errorf("level=%s format=%s: %v", tc.level, tc.format, err)
			continue
		}
		logger.Info("hello")
		if tc.contains != "" && !strings.Contains(buf.String(), tc.contains) {
			t.Errorf("level=%s format=%s: output %q lacks %q", tc.level, tc.format, buf.String(), tc.contains)
		}
		if strings.EqualFold(tc.level, "warn") && buf.Len() != 0 {
			t.Errorf("info message logged at warn level: %q", buf.String())
		}
	}
}
