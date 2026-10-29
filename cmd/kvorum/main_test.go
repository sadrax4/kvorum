package main

import (
	"testing"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

func TestParseInitialCluster(t *testing.T) {
	t.Parallel()
	got, err := parseInitialCluster(" 1=node1:2380, 2=node2:2380 ,3=node3:2380")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[raft.NodeID(2)] != "node2:2380" {
		t.Fatalf("parsed %v", got)
	}
	for _, bad := range []string{"1", "0=x:1", "1=x:1,1=y:2", "a=x:1", "1="} {
		if _, err := parseInitialCluster(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if got, err := parseInitialCluster(""); err != nil || len(got) != 0 {
		t.Fatalf("empty: %v, %v", got, err)
	}
}

func TestParseFlagsEnvFallback(t *testing.T) {
	t.Setenv("KVORUM_ID", "7")
	t.Setenv("KVORUM_TICK_INTERVAL", "250ms")
	t.Setenv("KVORUM_JOIN", "true")
	o, err := parseFlags([]string{"--data-dir", "/tmp/x", "--election-tick", "20"})
	if err != nil {
		t.Fatal(err)
	}
	if o.id != 7 || o.tickInterval != 250*time.Millisecond || !o.join || o.electionTick != 20 || o.dataDir != "/tmp/x" {
		t.Fatalf("options = %+v", o)
	}
	if _, err := parseFlags([]string{"extra"}); err == nil {
		t.Fatal("positional argument accepted")
	}
}
