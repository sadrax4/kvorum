package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

func entries(from, to raft.Index, term raft.Term) []raft.Entry {
	out := make([]raft.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: fmt.Appendf(nil, "entry-%d-%d", term, i)})
	}
	return out
}

func indexes(ents []raft.Entry) string {
	var b bytes.Buffer
	for i, e := range ents {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d/%d", e.Term, e.Index)
	}
	return b.String()
}

func mustOpen(t *testing.T, dir string, opts Options) (*WAL, Replay) {
	t.Helper()
	w, rp, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w, rp
}

func TestSaveAndReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, rp := mustOpen(t, dir, Options{})
	if len(rp.Entries) != 0 || !rp.HardState.IsEmpty() || rp.SnapshotMeta.Index != 0 {
		t.Fatalf("fresh log replayed state: %+v", rp)
	}
	hs := raft.HardState{Term: 3, Vote: 2, Commit: 4}
	if err := w.Save(hs, entries(1, 5, 1), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(raft.HardState{}, entries(6, 7, 2), false); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(raft.HardState{Term: 4, Vote: 1, Commit: 7}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(raft.HardState{}, entries(8, 8, 2), true); !errors.Is(err, ErrClosed) {
		t.Fatalf("save after close: %v", err)
	}

	w, rp = mustOpen(t, dir, Options{})
	defer w.Close()
	if rp.HardState != (raft.HardState{Term: 4, Vote: 1, Commit: 7}) {
		t.Fatalf("hard state = %+v", rp.HardState)
	}
	if got, want := indexes(rp.Entries), indexes(append(entries(1, 5, 1), entries(6, 7, 2)...)); got != want {
		t.Fatalf("entries = %s, want %s", got, want)
	}
	for i, e := range rp.Entries {
		if string(e.Data) != fmt.Sprintf("entry-%d-%d", e.Term, e.Index) {
			t.Fatalf("entry %d data = %q", i, e.Data)
		}
	}
}

func TestOverwriteSuffixOnReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	if err := w.Save(raft.HardState{Term: 1}, entries(1, 6, 1), true); err != nil {
		t.Fatal(err)
	}
	// A new leader replaces entries 4..6 with its own 4..5.
	if err := w.Save(raft.HardState{Term: 2}, entries(4, 5, 2), true); err != nil {
		t.Fatal(err)
	}
	// And then everything from 2 onwards.
	if err := w.Save(raft.HardState{Term: 3}, entries(2, 3, 3), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, rp := mustOpen(t, dir, Options{})
	want := append(entries(1, 1, 1), entries(2, 3, 3)...)
	if got := indexes(rp.Entries); got != indexes(want) {
		t.Fatalf("entries = %s, want %s", got, indexes(want))
	}
}

func TestSnapshotMarkerDropsCoveredEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	if err := w.Save(raft.HardState{Term: 1, Commit: 10}, entries(1, 10, 1), true); err != nil {
		t.Fatal(err)
	}
	meta := raft.SnapshotMeta{Index: 7, Term: 1, ConfState: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}
	if err := w.SaveSnapshot(meta); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(raft.HardState{}, entries(11, 12, 1), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, rp := mustOpen(t, dir, Options{})
	if rp.SnapshotMeta.Index != 7 || !rp.SnapshotMeta.ConfState.Equal(meta.ConfState) {
		t.Fatalf("snapshot meta = %+v", rp.SnapshotMeta)
	}
	if got := indexes(rp.Entries); got != indexes(entries(8, 12, 1)) {
		t.Fatalf("entries = %s", got)
	}
}

func TestSegmentCutAndRelease(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 512})
	var idx raft.Index = 1
	for range 20 {
		if err := w.Save(raft.HardState{Term: 1, Commit: idx + 4}, entries(idx, idx+4, 1), true); err != nil {
			t.Fatal(err)
		}
		idx += 5
	}
	if st := w.Stats(); st.Segments < 3 {
		t.Fatalf("expected several segments, got %d", st.Segments)
	}
	before := w.Stats().Segments
	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 60, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.ReleaseBefore(60); err != nil {
		t.Fatal(err)
	}
	after := w.Stats().Segments
	if after >= before {
		t.Fatalf("release did not remove segments: %d -> %d", before, after)
	}
	// Entries above the release point are still fully recoverable.
	if err := w.Save(raft.HardState{Term: 2, Commit: 100}, entries(101, 102, 2), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rp := mustOpen(t, dir, Options{SegmentSize: 512})
	defer w.Close()
	if rp.SnapshotMeta.Index != 60 {
		t.Fatalf("snapshot meta after reopen = %+v", rp.SnapshotMeta)
	}
	want := append(entries(61, 100, 1), entries(101, 102, 2)...)
	if got := indexes(rp.Entries); got != indexes(want) {
		t.Fatalf("entries after release/reopen = %s", got)
	}
	if rp.HardState.Commit != 100 || rp.HardState.Term != 2 {
		t.Fatalf("hard state after reopen = %+v", rp.HardState)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(files) != w.Stats().Segments {
		t.Fatalf("%d segment files on disk, stats say %d", len(files), w.Stats().Segments)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	if err := w.Save(raft.HardState{Term: 1, Commit: 3}, entries(1, 3, 1), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash in the middle of writing the next record by appending
	// a partial record.
	path := filepath.Join(dir, segmentName(1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		t.Fatal(err)
	}
	partial := appendRecord(nil, recordEntry, encodeEntry(raft.Entry{Index: 4, Term: 1, Data: []byte("lost")}))
	if _, err := f.Write(partial[:len(partial)-3]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w, rp := mustOpen(t, dir, Options{})
	if got := indexes(rp.Entries); got != indexes(entries(1, 3, 1)) {
		t.Fatalf("entries after torn tail = %s", got)
	}
	// The log keeps working from the truncation point.
	if err := w.Save(raft.HardState{Term: 1, Commit: 4}, entries(4, 4, 1), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, rp = mustOpen(t, dir, Options{})
	if got := indexes(rp.Entries); got != indexes(entries(1, 4, 1)) {
		t.Fatalf("entries after recovery = %s", got)
	}
}

func TestCorruptionInOlderSegmentFailsOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 256})
	var idx raft.Index = 1
	for range 10 {
		if err := w.Save(raft.HardState{Term: 1}, entries(idx, idx+3, 1), true); err != nil {
			t.Fatal(err)
		}
		idx += 4
	}
	if w.Stats().Segments < 2 {
		t.Fatal("expected at least two segments")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the middle of the first segment.
	path := filepath.Join(dir, segmentName(1))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xFF
	if err := os.WriteFile(path, b, filePerm); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open with corrupt older segment: err = %v, want ErrCorrupt", err)
	}
}

func TestStatsAndSyncObserver(t *testing.T) {
	t.Parallel()
	var syncs int
	var total time.Duration
	w, _ := mustOpen(t, t.TempDir(), Options{SyncObserver: func(d time.Duration) {
		syncs++
		total += d
	}})
	defer w.Close()
	if err := w.Save(raft.HardState{Term: 1}, entries(1, 2, 1), true); err != nil {
		t.Fatal(err)
	}
	if err := w.Save(raft.HardState{}, entries(3, 3, 1), false); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	st := w.Stats()
	// Opening cuts the first segment (one sync), then two explicit syncs.
	if st.Syncs != 3 || syncs != 3 || st.BytesWritten == 0 || st.Segments != 1 {
		t.Fatalf("stats = %+v, observer syncs = %d", st, syncs)
	}
	if total < 0 {
		t.Fatal("negative sync duration")
	}
}

func TestEmptySaveIsNoop(t *testing.T) {
	t.Parallel()
	w, _ := mustOpen(t, t.TempDir(), Options{})
	defer w.Close()
	before := w.Stats()
	if err := w.Save(raft.HardState{}, nil, true); err != nil {
		t.Fatal(err)
	}
	if after := w.Stats(); after != before {
		t.Fatalf("empty save changed stats: %+v -> %+v", before, after)
	}
}

func TestUnknownFilesAreIgnored(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), filePerm); err != nil {
		t.Fatal(err)
	}
	w, rp := mustOpen(t, dir, Options{})
	defer w.Close()
	if len(rp.Entries) != 0 {
		t.Fatalf("unexpected entries: %v", rp.Entries)
	}
}

func TestSequenceGapIsCorruption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentSize: 128})
	for i := raft.Index(1); i <= 30; i++ {
		if err := w.Save(raft.HardState{}, entries(i, i, 1), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Stats().Segments < 3 {
		t.Fatal("expected at least three segments")
	}
	if err := os.Remove(filepath.Join(dir, segmentName(2))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func FuzzReadRecord(f *testing.F) {
	f.Add(appendRecord(nil, recordEntry, encodeEntry(raft.Entry{Index: 1, Term: 1, Data: []byte("x")})))
	f.Add(appendRecord(nil, recordHardState, encodeHardState(raft.HardState{Term: 1})))
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0x7F, 1, 2, 3, 4, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := newReader(data)
		typ, payload, err := readRecord(r)
		if err != nil {
			return
		}
		switch typ {
		case recordEntry:
			_, _ = decodeEntry(payload)
		case recordHardState:
			_, _ = decodeHardState(payload)
		case recordSnapshot:
			_, _ = decodeSnapshotMeta(payload)
		case recordHeader:
			_ = decodeHeader(payload)
		}
	})
}

func BenchmarkSave(b *testing.B) {
	for _, sync := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync=%t", sync), func(b *testing.B) {
			w, _ := mustOpenB(b, b.TempDir())
			defer w.Close()
			data := make([]byte, 256)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				e := raft.Entry{Index: raft.Index(i + 1), Term: 1, Data: data}
				if err := w.Save(raft.HardState{Term: 1, Commit: e.Index}, []raft.Entry{e}, sync); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func mustOpenB(b *testing.B, dir string) (*WAL, Replay) {
	b.Helper()
	w, rp, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	return w, rp
}
