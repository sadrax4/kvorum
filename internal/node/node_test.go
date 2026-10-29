package node

import (
	"bytes"
	"testing"

	"github.com/sadrax4/kvorum/raft"
)

func TestMemberContextRoundTrip(t *testing.T) {
	t.Parallel()
	mc := memberContext{RaftAddress: "10.0.0.1:2380", ClientAddress: "10.0.0.1:2379"}
	got, err := decodeMemberContext(encodeMemberContext(mc))
	if err != nil || got != mc {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	if got, err := decodeMemberContext(nil); err != nil || got != (memberContext{}) {
		t.Fatalf("empty context = %+v, %v", got, err)
	}
	if _, err := decodeMemberContext([]byte("{not json")); err == nil {
		t.Fatal("malformed context accepted")
	}
}

func TestMemberAttrsRoundTrip(t *testing.T) {
	t.Parallel()
	a := memberAttrs{ID: 3, ClientAddress: "host:2379"}
	got, err := decodeMemberAttrs(encodeMemberAttrs(a))
	if err != nil || got != a {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	if _, err := decodeMemberAttrs(encodeMemberAttrs(memberAttrs{ClientAddress: "x"})); err == nil {
		t.Fatal("attributes without id accepted")
	}
}

func TestSnapshotEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	members := snapshotMembers{
		1: {RaftAddress: "a:1", ClientAddress: "a:2"},
		2: {RaftAddress: "b:1"},
	}
	store := []byte("store-payload")
	data, err := encodeSnapshot(members, store)
	if err != nil {
		t.Fatal(err)
	}
	gotMembers, gotStore, err := decodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotMembers) != 2 || gotMembers[1] != members[1] || gotMembers[2] != members[2] || !bytes.Equal(gotStore, store) {
		t.Fatalf("decoded %+v / %q", gotMembers, gotStore)
	}
	empty, err := encodeSnapshot(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m, s, err := decodeSnapshot(empty); err != nil || len(m) != 0 || len(s) != 0 {
		t.Fatalf("empty envelope = %v / %q / %v", m, s, err)
	}
	for _, bad := range [][]byte{nil, []byte("junk"), data[:len(snapshotMagic)+2]} {
		if _, _, err := decodeSnapshot(bad); err == nil {
			t.Fatalf("malformed envelope %q accepted", bad)
		}
	}
}

func TestEntryKindEncoding(t *testing.T) {
	t.Parallel()
	e := encodeEntry(entryKindCommand, []byte("cmd"))
	if e[0] != entryKindCommand || string(e[1:]) != "cmd" {
		t.Fatalf("encoded entry = %v", e)
	}
}

func TestReadContextRoundTrip(t *testing.T) {
	t.Parallel()
	for _, id := range []uint64{0, 1, 255, 1 << 40, ^uint64(0)} {
		got, ok := decodeReadContext(encodeReadContext(id))
		if !ok || got != id {
			t.Fatalf("round trip %d = %d, %t", id, got, ok)
		}
	}
	if _, ok := decodeReadContext([]byte{1, 2, 3}); ok {
		t.Fatal("short context accepted")
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	base := Config{ID: 1, DataDir: "/tmp/x", ListenRaft: "127.0.0.1:0", InitialCluster: map[raft.NodeID]string{1: "127.0.0.1:0"}}
	if _, err := base.withDefaults(); err == nil {
		t.Fatal("missing Metrics accepted")
	}
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero id", func(c *Config) { c.ID = 0 }},
		{"no data dir", func(c *Config) { c.DataDir = "" }},
		{"no listen", func(c *Config) { c.ListenRaft = "" }},
		{"no cluster", func(c *Config) { c.InitialCluster = nil }},
		{"join without peers", func(c *Config) { c.Join = true; c.InitialCluster = nil }},
	}
	for _, tc := range cases {
		cfg := base
		tc.mutate(&cfg)
		if _, err := cfg.withDefaults(); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
}

func TestBootstrapPeersDeterministic(t *testing.T) {
	t.Parallel()
	initial := map[raft.NodeID]string{3: "c", 1: "a", 2: "b"}
	first := bootstrapPeers(initial)
	for range 20 {
		again := bootstrapPeers(initial)
		if len(again) != 3 {
			t.Fatalf("got %d peers", len(again))
		}
		for i := range again {
			if again[i].ID != first[i].ID || !bytes.Equal(again[i].Context, first[i].Context) {
				t.Fatalf("bootstrap peers differ between calls: %v vs %v", again, first)
			}
		}
	}
	if first[0].ID != 1 || first[1].ID != 2 || first[2].ID != 3 {
		t.Fatalf("peers not sorted: %v", first)
	}
	mc, err := decodeMemberContext(first[2].Context)
	if err != nil || mc.RaftAddress != "c" || mc.ClientAddress != "" {
		t.Fatalf("context = %+v, %v", mc, err)
	}
}
