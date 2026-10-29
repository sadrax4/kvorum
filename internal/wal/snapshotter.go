package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/sadrax4/kvorum/raft"
)

// ErrNoSnapshot is returned by Load when no valid snapshot exists.
var ErrNoSnapshot = errors.New("wal: no snapshot")

const (
	snapshotSuffix = ".snap"
	tempSuffix     = ".tmp"
)

var snapshotMagic = []byte("KVSNAP")

// Snapshotter stores snapshots as individual files named
// <term>-<index>.snap.
type Snapshotter struct {
	dir    string
	logger *slog.Logger
}

// NewSnapshotter opens (creating if needed) the snapshot directory.
func NewSnapshotter(dir string, logger *slog.Logger) (*Snapshotter, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("wal: create snapshot directory: %w", err)
	}
	return &Snapshotter{dir: dir, logger: logger.With("component", "snapshotter")}, nil
}

func snapshotName(meta raft.SnapshotMeta) string {
	return fmt.Sprintf("%016x-%016x%s", uint64(meta.Term), uint64(meta.Index), snapshotSuffix)
}

// parseSnapshotName returns the term and index encoded in a snapshot file
// name.
func parseSnapshotName(name string) (term raft.Term, index raft.Index, ok bool) {
	base := strings.TrimSuffix(name, snapshotSuffix)
	parts := strings.Split(base, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	t, err1 := strconv.ParseUint(parts[0], 16, 64)
	i, err2 := strconv.ParseUint(parts[1], 16, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return raft.Term(t), raft.Index(i), true
}

// Save writes the snapshot atomically: to a temporary file, fsynced, then
// renamed into place, followed by a directory fsync.
func (s *Snapshotter) Save(snap raft.Snapshot) error {
	if snap.IsEmpty() {
		return errors.New("wal: refusing to save an empty snapshot")
	}
	final := filepath.Join(s.dir, snapshotName(snap.Meta))
	tmp := final + tempSuffix

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("wal: create snapshot file: %w", err)
	}
	if _, err := f.Write(encodeSnapshotFile(snap)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("wal: write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("wal: sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("wal: close snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("wal: rename snapshot: %w", err)
	}
	return syncDir(s.dir)
}

// Load returns the newest valid snapshot. Corrupt files are skipped with a
// warning so that an older valid snapshot can still be used.
func (s *Snapshotter) Load() (raft.Snapshot, error) {
	names, err := s.sortedNames()
	if err != nil {
		return raft.Snapshot{}, err
	}
	for _, name := range names {
		snap, err := readSnapshotFile(filepath.Join(s.dir, name))
		if err != nil {
			s.logger.Warn("skipping unreadable snapshot", "file", name, "error", err)
			continue
		}
		return snap, nil
	}
	return raft.Snapshot{}, ErrNoSnapshot
}

// Prune keeps the newest keep snapshot files and removes the rest, along
// with any leftover temporary files.
func (s *Snapshotter) Prune(keep int) error {
	names, err := s.sortedNames()
	if err != nil {
		return err
	}
	removed := 0
	for i, name := range names {
		if i < keep {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: remove snapshot %s: %w", name, err)
		}
		removed++
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("wal: read snapshot directory: %w", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tempSuffix) {
			if err := os.Remove(filepath.Join(s.dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("wal: remove temporary file %s: %w", e.Name(), err)
			}
			removed++
		}
	}
	if removed == 0 {
		return nil
	}
	return syncDir(s.dir)
}

// sortedNames lists snapshot files, newest (highest index, then term) first.
func (s *Snapshotter) sortedNames() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read snapshot directory: %w", err)
	}
	type named struct {
		name  string
		term  raft.Term
		index raft.Index
	}
	var files []named
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotSuffix) {
			continue
		}
		term, index, ok := parseSnapshotName(e.Name())
		if !ok {
			s.logger.Warn("ignoring unrecognised file in snapshot directory", "file", e.Name())
			continue
		}
		files = append(files, named{name: e.Name(), term: term, index: index})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].index != files[j].index {
			return files[i].index > files[j].index
		}
		return files[i].term > files[j].term
	})
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.name)
	}
	return names, nil
}

// Snapshot file layout:
//
//	magic "KVSNAP" | version u8 | crc32c u32 (over payload) | payload
//	payload := index u64 | term u64 | conf state len u32 | conf state | data len u64 | data
func encodeSnapshotFile(snap raft.Snapshot) []byte {
	cs, err := snap.Meta.ConfState.MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("wal: marshal conf state: %v", err)) // cannot fail
	}
	payload := make([]byte, 0, 8+8+4+len(cs)+8+len(snap.Data))
	payload = binary.LittleEndian.AppendUint64(payload, uint64(snap.Meta.Index))
	payload = binary.LittleEndian.AppendUint64(payload, uint64(snap.Meta.Term))
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(cs)))
	payload = append(payload, cs...)
	payload = binary.LittleEndian.AppendUint64(payload, uint64(len(snap.Data)))
	payload = append(payload, snap.Data...)

	out := make([]byte, 0, len(snapshotMagic)+1+4+len(payload))
	out = append(out, snapshotMagic...)
	out = append(out, formatVersion)
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(payload, crcTable))
	return append(out, payload...)
}

func decodeSnapshotFile(b []byte) (raft.Snapshot, error) {
	header := len(snapshotMagic) + 1 + 4
	if len(b) < header || string(b[:len(snapshotMagic)]) != string(snapshotMagic) {
		return raft.Snapshot{}, errors.New("bad snapshot magic")
	}
	if v := b[len(snapshotMagic)]; v != formatVersion {
		return raft.Snapshot{}, fmt.Errorf("unsupported snapshot format version %d", v)
	}
	crc := binary.LittleEndian.Uint32(b[len(snapshotMagic)+1:])
	payload := b[header:]
	if crc32.Checksum(payload, crcTable) != crc {
		return raft.Snapshot{}, errBadChecksum
	}
	if len(payload) < 8+8+4 {
		return raft.Snapshot{}, errors.New("snapshot payload too short")
	}
	var snap raft.Snapshot
	snap.Meta.Index = raft.Index(binary.LittleEndian.Uint64(payload[0:8]))
	snap.Meta.Term = raft.Term(binary.LittleEndian.Uint64(payload[8:16]))
	csLen := binary.LittleEndian.Uint32(payload[16:20])
	payload = payload[20:]
	if uint64(len(payload)) < uint64(csLen)+8 {
		return raft.Snapshot{}, errors.New("snapshot payload truncated")
	}
	if err := snap.Meta.ConfState.UnmarshalBinary(payload[:csLen]); err != nil {
		return raft.Snapshot{}, fmt.Errorf("snapshot conf state: %w", err)
	}
	payload = payload[csLen:]
	dataLen := binary.LittleEndian.Uint64(payload[:8])
	payload = payload[8:]
	if uint64(len(payload)) != dataLen {
		return raft.Snapshot{}, fmt.Errorf("snapshot data has %d bytes, header says %d", len(payload), dataLen)
	}
	if dataLen > 0 {
		snap.Data = append([]byte(nil), payload...)
	}
	return snap, nil
}

func readSnapshotFile(path string) (raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, err
	}
	return decodeSnapshotFile(b)
}
