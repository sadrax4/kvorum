package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sadrax4/kvorum/raft"
)

func TestSnapshotterSaveLoadPrune(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewSnapshotter(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("load on empty dir: err = %v", err)
	}
	if err := s.Save(raft.Snapshot{}); err == nil {
		t.Fatal("saving an empty snapshot succeeded")
	}
	cs := raft.ConfState{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{4}}
	for i := raft.Index(10); i <= 30; i += 10 {
		snap := raft.Snapshot{Meta: raft.SnapshotMeta{Index: i, Term: 2, ConfState: cs}, Data: bytes.Repeat([]byte{byte(i)}, 1000)}
		if err := s.Save(snap); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta.Index != 30 || got.Meta.Term != 2 || !got.Meta.ConfState.Equal(cs) || !bytes.Equal(got.Data, bytes.Repeat([]byte{30}, 1000)) {
		t.Fatalf("loaded snapshot = %+v", got.Meta)
	}

	// A stray temp file and a corrupt newest file are tolerated.
	if err := os.WriteFile(filepath.Join(dir, "junk.snap.tmp"), []byte("x"), filePerm); err != nil {
		t.Fatal(err)
	}
	newest := filepath.Join(dir, snapshotName(raft.SnapshotMeta{Index: 40, Term: 2}))
	if err := os.WriteFile(newest, []byte("garbage"), filePerm); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load()
	if err != nil || got.Meta.Index != 30 {
		t.Fatalf("load with corrupt newest: %+v, %v", got.Meta, err)
	}

	if err := s.Prune(1); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	// Prune keeps the newest file by name (the corrupt one) and removes the
	// rest; the temp file is gone too.
	if len(names) != 1 || names[0] != filepath.Base(newest) {
		t.Fatalf("files after prune = %v", names)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("load after pruning valid files: err = %v", err)
	}
}

func TestSnapshotFileRoundTrip(t *testing.T) {
	t.Parallel()
	snap := raft.Snapshot{
		Meta: raft.SnapshotMeta{Index: 7, Term: 3, ConfState: raft.ConfState{Voters: []raft.NodeID{9}}},
		Data: []byte("payload"),
	}
	b := encodeSnapshotFile(snap)
	got, err := decodeSnapshotFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta.Index != 7 || got.Meta.Term != 3 || !got.Meta.ConfState.Equal(snap.Meta.ConfState) || string(got.Data) != "payload" {
		t.Fatalf("round trip = %+v", got)
	}
	b[len(b)-1] ^= 1
	if _, err := decodeSnapshotFile(b); !errors.Is(err, errBadChecksum) {
		t.Fatalf("corrupt data: err = %v", err)
	}
	if _, err := decodeSnapshotFile(b[:10]); err == nil {
		t.Fatal("truncated file accepted")
	}
	if _, err := decodeSnapshotFile([]byte("NOPE")); err == nil {
		t.Fatal("bad magic accepted")
	}
}

func TestParseSnapshotName(t *testing.T) {
	t.Parallel()
	meta := raft.SnapshotMeta{Index: 0xabc, Term: 0x12}
	term, index, ok := parseSnapshotName(snapshotName(meta))
	if !ok || term != 0x12 || index != 0xabc {
		t.Fatalf("parse = %d/%d/%t", term, index, ok)
	}
	if _, _, ok := parseSnapshotName("weird.snap"); ok {
		t.Fatal("parsed a malformed name")
	}
}
