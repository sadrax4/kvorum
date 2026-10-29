package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sadrax4/kvorum/raft"
)

var (
	// ErrCorrupt is returned by Open when a segment other than the newest one
	// contains an unreadable record.
	ErrCorrupt = errors.New("wal: corrupt log")
	// ErrClosed is returned by operations on a closed WAL.
	ErrClosed = errors.New("wal: closed")
)

const (
	defaultSegmentSize = 64 << 20
	segmentSuffix      = ".wal"
	dirPerm            = 0o700
	filePerm           = 0o600
)

// Options configures a WAL.
type Options struct {
	// SegmentSize is the size in bytes beyond which a new segment is cut.
	// Zero selects 64 MiB.
	SegmentSize int64
	// SyncObserver, if set, is called after every fsync with its duration.
	SyncObserver func(time.Duration)
	// Logger receives diagnostics. Nil disables logging.
	Logger *slog.Logger
}

// Replay is the state recovered from the log by Open.
type Replay struct {
	// HardState is the last persisted hard state.
	HardState raft.HardState
	// Entries are the entries after SnapshotMeta.Index, contiguous and with
	// conflicting suffixes already resolved.
	Entries []raft.Entry
	// SnapshotMeta is the most recent snapshot marker, or the zero value.
	SnapshotMeta raft.SnapshotMeta
}

// Stats reports counters about the log.
type Stats struct {
	Segments     int
	BytesWritten uint64
	Syncs        uint64
}

type segment struct {
	seq            uint64
	path           string
	size           int64
	lastEntryIndex raft.Index
	hasEntries     bool
}

// WAL is a segmented write-ahead log. It is safe for concurrent use, though
// the node drives it from a single goroutine.
type WAL struct {
	dir    string
	opts   Options
	logger *slog.Logger

	mu       sync.Mutex
	segments []*segment // ascending by seq; the last one is active
	file     *os.File
	w        *bufio.Writer
	closed   bool

	hardState    raft.HardState
	snapshotMeta raft.SnapshotMeta
	stats        Stats
	buf          []byte
}

// Open opens the log in dir, creating the directory and the first segment if
// needed, and replays the existing segments.
func Open(dir string, opts Options) (*WAL, Replay, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, Replay{}, fmt.Errorf("wal: create directory: %w", err)
	}
	w := &WAL{dir: dir, opts: opts, logger: opts.Logger.With("component", "wal")}

	segs, err := w.listSegments()
	if err != nil {
		return nil, Replay{}, err
	}
	replay, err := w.replay(segs)
	if err != nil {
		return nil, Replay{}, err
	}
	w.segments = segs
	w.hardState = replay.HardState
	w.snapshotMeta = replay.SnapshotMeta

	if len(segs) == 0 {
		if err := w.cutLocked(); err != nil {
			return nil, Replay{}, err
		}
	} else {
		active := segs[len(segs)-1]
		f, err := os.OpenFile(active.path, os.O_WRONLY|os.O_APPEND, filePerm)
		if err != nil {
			return nil, Replay{}, fmt.Errorf("wal: open active segment: %w", err)
		}
		w.file = f
		w.w = bufio.NewWriterSize(f, 1<<20)
	}
	w.stats.Segments = len(w.segments)
	return w, replay, nil
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("%016x%s", seq, segmentSuffix)
}

func (w *WAL) listSegments() ([]*segment, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read directory: %w", err)
	}
	var segs []*segment
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 16, 64)
		if err != nil {
			w.logger.Warn("ignoring unrecognised file in wal directory", "file", name)
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("wal: stat %s: %w", name, err)
		}
		segs = append(segs, &segment{seq: seq, path: filepath.Join(w.dir, name), size: info.Size()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	for i := 1; i < len(segs); i++ {
		if segs[i].seq != segs[i-1].seq+1 {
			return nil, fmt.Errorf("%w: segment sequence gap between %016x and %016x", ErrCorrupt, segs[i-1].seq, segs[i].seq)
		}
	}
	return segs, nil
}

// replay reads every segment and reconstructs the recovered state. The
// newest segment may end with a torn record, which is truncated away.
func (w *WAL) replay(segs []*segment) (Replay, error) {
	var rp Replay
	for i, seg := range segs {
		last := i == len(segs)-1
		if err := w.replaySegment(seg, last, &rp); err != nil {
			return Replay{}, err
		}
	}
	return rp, nil
}

func (w *WAL) replaySegment(seg *segment, last bool, rp *Replay) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return fmt.Errorf("wal: open %s: %w", seg.path, err)
	}
	defer f.Close() //nolint:errcheck // read-only file; nothing to flush

	r := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	first := true
	for {
		typ, payload, err := readRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if !last {
				return fmt.Errorf("%w: %s at offset %d: %w", ErrCorrupt, filepath.Base(seg.path), offset, err)
			}
			w.logger.Warn("truncating torn tail of newest segment", "file", filepath.Base(seg.path), "offset", offset, "error", err)
			if err := os.Truncate(seg.path, offset); err != nil {
				return fmt.Errorf("wal: truncate %s: %w", seg.path, err)
			}
			seg.size = offset
			if err := syncDir(w.dir); err != nil {
				return err
			}
			break
		}
		recSize := int64(recordPrefixSize + len(payload))
		if first {
			if typ != recordHeader {
				return fmt.Errorf("%w: %s does not start with a header record", ErrCorrupt, filepath.Base(seg.path))
			}
			if err := decodeHeader(payload); err != nil {
				return fmt.Errorf("%w: %s: %w", ErrCorrupt, filepath.Base(seg.path), err)
			}
			first = false
			offset += recSize
			continue
		}
		if err := applyRecord(typ, payload, seg, rp); err != nil {
			return fmt.Errorf("%w: %s at offset %d: %w", ErrCorrupt, filepath.Base(seg.path), offset, err)
		}
		offset += recSize
	}
	if first && seg.size > 0 {
		return fmt.Errorf("%w: %s has no header record", ErrCorrupt, filepath.Base(seg.path))
	}
	return nil
}

func applyRecord(typ byte, payload []byte, seg *segment, rp *Replay) error {
	switch typ {
	case recordHeader:
		return errors.New("unexpected header record")
	case recordHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return err
		}
		rp.HardState = hs
	case recordSnapshot:
		meta, err := decodeSnapshotMeta(payload)
		if err != nil {
			return err
		}
		if meta.Index >= rp.SnapshotMeta.Index {
			rp.SnapshotMeta = meta
			rp.Entries = dropThrough(rp.Entries, meta.Index)
		}
	case recordEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		if e.Index > seg.lastEntryIndex {
			seg.lastEntryIndex = e.Index
		}
		seg.hasEntries = true
		if e.Index <= rp.SnapshotMeta.Index {
			return nil
		}
		if n := len(rp.Entries); n > 0 {
			first := rp.Entries[0].Index
			switch {
			case e.Index <= first:
				rp.Entries = rp.Entries[:0]
			case e.Index <= rp.Entries[n-1].Index:
				rp.Entries = rp.Entries[:e.Index-first]
			case e.Index != rp.Entries[n-1].Index+1:
				return fmt.Errorf("entry %d follows %d: gap in log", e.Index, rp.Entries[n-1].Index)
			}
		}
		rp.Entries = append(rp.Entries, e)
	default:
		return fmt.Errorf("unknown record type %#x", typ)
	}
	return nil
}

// dropThrough removes entries with index at or below idx.
func dropThrough(ents []raft.Entry, idx raft.Index) []raft.Entry {
	i := 0
	for i < len(ents) && ents[i].Index <= idx {
		i++
	}
	return ents[i:]
}

// Save appends entries and, if non-empty, the hard state, then flushes and
// optionally fsyncs. Entries at or below the last written index overwrite the
// suffix on replay.
func (w *WAL) Save(hs raft.HardState, ents []raft.Entry, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if len(ents) == 0 && hs.IsEmpty() {
		return nil
	}
	if err := w.maybeCutLocked(); err != nil {
		return err
	}
	active := w.segments[len(w.segments)-1]
	w.buf = w.buf[:0]
	for _, e := range ents {
		w.buf = appendRecord(w.buf, recordEntry, encodeEntry(e))
		if e.Index > active.lastEntryIndex {
			active.lastEntryIndex = e.Index
		}
		active.hasEntries = true
	}
	if !hs.IsEmpty() {
		w.buf = appendRecord(w.buf, recordHardState, encodeHardState(hs))
		w.hardState = hs
	}
	if err := w.writeLocked(w.buf); err != nil {
		return err
	}
	if sync {
		return w.syncLocked()
	}
	return nil
}

// SaveSnapshot records that a snapshot covering meta.Index exists. The
// record is fsynced before returning.
func (w *WAL) SaveSnapshot(meta raft.SnapshotMeta) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := w.maybeCutLocked(); err != nil {
		return err
	}
	w.buf = appendRecord(w.buf[:0], recordSnapshot, encodeSnapshotMeta(meta))
	if err := w.writeLocked(w.buf); err != nil {
		return err
	}
	w.snapshotMeta = meta
	return w.syncLocked()
}

// Sync flushes buffered records and fsyncs the active segment.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.syncLocked()
}

// ReleaseBefore deletes every segment, other than the active one, whose
// entries all lie below index. Callers pass an index at or below the latest
// snapshot so that the released entries are recoverable from it.
func (w *WAL) ReleaseBefore(index raft.Index) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	keep := 0
	for keep < len(w.segments)-1 {
		seg := w.segments[keep]
		if seg.hasEntries && seg.lastEntryIndex >= index {
			break
		}
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: remove %s: %w", seg.path, err)
		}
		keep++
	}
	if keep == 0 {
		return nil
	}
	w.segments = append([]*segment(nil), w.segments[keep:]...)
	w.stats.Segments = len(w.segments)
	w.logger.Info("released wal segments", "count", keep, "before_index", index)
	return syncDir(w.dir)
}

// Close flushes, fsyncs, and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.syncLocked()
	if cerr := w.file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// Dir returns the log directory.
func (w *WAL) Dir() string { return w.dir }

// Stats returns counters about the log.
func (w *WAL) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *WAL) writeLocked(b []byte) error {
	n, err := w.w.Write(b)
	w.stats.BytesWritten += uint64(n)
	w.segments[len(w.segments)-1].size += int64(n)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	return nil
}

func (w *WAL) syncLocked() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	start := time.Now()
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.stats.Syncs++
	if w.opts.SyncObserver != nil {
		w.opts.SyncObserver(time.Since(start))
	}
	return nil
}

func (w *WAL) maybeCutLocked() error {
	if w.segments[len(w.segments)-1].size < w.opts.SegmentSize {
		return nil
	}
	return w.cutLocked()
}

// cutLocked starts a new segment that begins with the state needed to make
// the previous segments releasable.
func (w *WAL) cutLocked() error {
	if w.file != nil {
		if err := w.syncLocked(); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("wal: close segment: %w", err)
		}
		w.file = nil
	}
	seq := uint64(1)
	if n := len(w.segments); n > 0 {
		seq = w.segments[n-1].seq + 1
	}
	seg := &segment{seq: seq, path: filepath.Join(w.dir, segmentName(seq))}
	f, err := os.OpenFile(seg.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	w.file = f
	w.w = bufio.NewWriterSize(f, 1<<20)
	w.segments = append(w.segments, seg)
	w.stats.Segments = len(w.segments)

	w.buf = appendRecord(w.buf[:0], recordHeader, encodeHeader())
	if !w.hardState.IsEmpty() {
		w.buf = appendRecord(w.buf, recordHardState, encodeHardState(w.hardState))
	}
	if w.snapshotMeta.Index > 0 {
		w.buf = appendRecord(w.buf, recordSnapshot, encodeSnapshotMeta(w.snapshotMeta))
	}
	if err := w.writeLocked(w.buf); err != nil {
		return err
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	w.logger.Debug("cut new wal segment", "seq", seq)
	return syncDir(w.dir)
}

// syncDir fsyncs a directory so that file creations, renames, and removals
// in it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open directory for sync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("wal: sync directory: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("wal: close directory: %w", err)
	}
	return nil
}
