package node

import (
	"errors"
	"fmt"
	"time"

	"github.com/sadrax4/kvorum/internal/wal"
	"github.com/sadrax4/kvorum/raft"
)

// openStorage opens the write-ahead log and snapshot store, replays them
// into the in-memory raft storage and the key-value store, and reports
// whether the node is brand new. It returns the index the application has
// applied (the snapshot index), which raft needs to avoid redelivering
// entries that are already reflected in the snapshot.
func (n *Node) openStorage(walDir, snapDir string) (fresh bool, applied raft.Index, err error) {
	snapshotter, err := wal.NewSnapshotter(snapDir, n.logger)
	if err != nil {
		return false, 0, fmt.Errorf("node: open snapshot store: %w", err)
	}
	n.snapshotter = snapshotter

	w, replay, err := wal.Open(walDir, wal.Options{
		SegmentSize: n.cfg.WALSegmentSize,
		SyncObserver: func(d time.Duration) {
			n.metrics.WALSyncDuration.Observe(d.Seconds())
		},
		Logger: n.logger,
	})
	if err != nil {
		return false, 0, fmt.Errorf("node: open wal: %w", err)
	}
	n.wal = w
	n.raftStorage = raft.NewMemoryStorage()

	snap, err := snapshotter.Load()
	switch {
	case errors.Is(err, wal.ErrNoSnapshot):
		snap = raft.Snapshot{}
	case err != nil:
		_ = w.Close()
		return false, 0, fmt.Errorf("node: load snapshot: %w", err)
	}
	if replay.SnapshotMeta.Index > snap.Meta.Index {
		_ = w.Close()
		return false, 0, fmt.Errorf("node: wal references snapshot %d but the newest snapshot file is %d",
			replay.SnapshotMeta.Index, snap.Meta.Index)
	}

	if !snap.IsEmpty() {
		if err := n.raftStorage.ApplySnapshot(snap); err != nil {
			_ = w.Close()
			return false, 0, fmt.Errorf("node: apply snapshot to storage: %w", err)
		}
		if err := n.restoreApplicationState(snap); err != nil {
			_ = w.Close()
			return false, 0, err
		}
		applied = snap.Meta.Index
		n.snapshotIndex.Store(uint64(snap.Meta.Index))
		if snap.Meta.Index > replay.SnapshotMeta.Index {
			// The snapshot file was made durable but the crash came before
			// its marker reached the log. Record it now, otherwise entries
			// replayed from before the snapshot and entries appended after
			// it would leave a gap on the next restart.
			n.logger.Warn("recording snapshot marker missing from the wal", "snapshot_index", snap.Meta.Index)
			if err := w.SaveSnapshot(snap.Meta); err != nil {
				_ = w.Close()
				return false, 0, fmt.Errorf("node: record snapshot %d in wal: %w", snap.Meta.Index, err)
			}
		}
	}

	// Entries covered by the snapshot are redundant; the rest must continue
	// the log directly after it.
	entries := replay.Entries
	for len(entries) > 0 && entries[0].Index <= snap.Meta.Index {
		entries = entries[1:]
	}
	if len(entries) > 0 {
		if first := entries[0].Index; first > snap.Meta.Index+1 {
			_ = w.Close()
			return false, 0, fmt.Errorf("node: wal entries start at %d but the snapshot ends at %d", first, snap.Meta.Index)
		}
		if err := n.raftStorage.Append(entries); err != nil {
			_ = w.Close()
			return false, 0, fmt.Errorf("node: replay entries: %w", err)
		}
	}
	if !replay.HardState.IsEmpty() {
		n.raftStorage.SetHardState(replay.HardState)
	}

	last, _ := n.raftStorage.LastIndex()
	fresh = last == 0 && snap.IsEmpty() && replay.HardState.IsEmpty()
	n.logger.Info("storage opened",
		"fresh", fresh, "snapshot_index", snap.Meta.Index, "entries", len(entries),
		"last_index", last, "hard_state_term", replay.HardState.Term, "hard_state_commit", replay.HardState.Commit)
	return fresh, applied, nil
}

// restoreApplicationState loads the membership table and the key-value
// store from a snapshot payload.
func (n *Node) restoreApplicationState(snap raft.Snapshot) error {
	members, storeData, err := decodeSnapshot(snap.Data)
	if err != nil {
		return fmt.Errorf("node: decode snapshot %d: %w", snap.Meta.Index, err)
	}
	if err := n.store.Restore(storeData); err != nil {
		return fmt.Errorf("node: restore store from snapshot %d: %w", snap.Meta.Index, err)
	}
	n.mu.Lock()
	n.members = members
	n.confState = snap.Meta.ConfState.Clone()
	n.mu.Unlock()
	n.appliedIndex.Store(uint64(snap.Meta.Index))
	return nil
}

// persistReady writes the durable parts of a Ready in the order the raft
// contract requires: snapshot first, then entries and hard state.
func (n *Node) persistReady(rd raft.Ready) error {
	if !rd.Snapshot.IsEmpty() {
		if err := n.installSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) == 0 && rd.HardState.IsEmpty() {
		return nil
	}
	if err := n.wal.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
		return fmt.Errorf("node: wal save: %w", err)
	}
	if err := n.raftStorage.Append(rd.Entries); err != nil {
		return fmt.Errorf("node: storage append: %w", err)
	}
	if !rd.HardState.IsEmpty() {
		n.raftStorage.SetHardState(rd.HardState)
	}
	return nil
}

// installSnapshot persists a snapshot received from the leader and resets
// the application state to it.
func (n *Node) installSnapshot(snap raft.Snapshot) error {
	n.logger.Info("installing snapshot from leader", "index", snap.Meta.Index, "term", snap.Meta.Term, "bytes", len(snap.Data))
	if err := n.snapshotter.Save(snap); err != nil {
		return fmt.Errorf("node: save snapshot %d: %w", snap.Meta.Index, err)
	}
	if err := n.wal.SaveSnapshot(snap.Meta); err != nil {
		return fmt.Errorf("node: record snapshot %d in wal: %w", snap.Meta.Index, err)
	}
	if err := n.raftStorage.ApplySnapshot(snap); err != nil {
		return fmt.Errorf("node: apply snapshot %d to storage: %w", snap.Meta.Index, err)
	}
	if err := n.restoreApplicationState(snap); err != nil {
		return err
	}
	n.snapshotIndex.Store(uint64(snap.Meta.Index))
	n.syncPeers()
	n.failWaiters(ErrProposalLost)
	n.metrics.RaftSnapshots.WithLabelValues("installed").Inc()
	// The in-memory log already starts after the snapshot; release the WAL
	// segments that only hold entries the snapshot has superseded.
	if err := n.wal.ReleaseBefore(snap.Meta.Index); err != nil {
		return fmt.Errorf("node: release wal before %d: %w", snap.Meta.Index, err)
	}
	n.metrics.WALSegments.Set(float64(n.wal.Stats().Segments))
	return n.snapshotter.Prune(2)
}

// takeSnapshot captures the application state at the applied index, persists
// it, and compacts the log.
func (n *Node) takeSnapshot() (raft.Index, error) {
	applied := raft.Index(n.appliedIndex.Load())
	if applied == 0 || applied <= raft.Index(n.snapshotIndex.Load()) {
		return raft.Index(n.snapshotIndex.Load()), nil
	}
	start := time.Now()
	storeData, err := n.store.Snapshot()
	if err != nil {
		return 0, fmt.Errorf("node: snapshot store: %w", err)
	}
	n.mu.RLock()
	members := make(snapshotMembers, len(n.members))
	for id, mc := range n.members {
		members[id] = mc
	}
	confState := n.confState.Clone()
	n.mu.RUnlock()

	data, err := encodeSnapshot(members, storeData)
	if err != nil {
		return 0, fmt.Errorf("node: encode snapshot: %w", err)
	}
	snap, err := n.raftStorage.CreateSnapshot(applied, confState, data)
	if err != nil {
		return 0, fmt.Errorf("node: create snapshot at %d: %w", applied, err)
	}
	if err := n.snapshotter.Save(snap); err != nil {
		return 0, fmt.Errorf("node: save snapshot %d: %w", applied, err)
	}
	if err := n.wal.SaveSnapshot(snap.Meta); err != nil {
		return 0, fmt.Errorf("node: record snapshot %d in wal: %w", applied, err)
	}
	n.snapshotIndex.Store(uint64(applied))
	n.metrics.RaftSnapshots.WithLabelValues("created").Inc()
	n.metrics.RaftSnapshotIndex.Set(float64(applied))

	if applied > raft.Index(n.cfg.SnapshotCatchUpEntries) {
		if err := n.compactTo(applied - raft.Index(n.cfg.SnapshotCatchUpEntries)); err != nil {
			return 0, err
		}
	}
	if err := n.snapshotter.Prune(2); err != nil {
		return 0, fmt.Errorf("node: prune snapshots: %w", err)
	}
	n.logger.Info("snapshot taken", "index", applied, "bytes", len(data), "duration", time.Since(start))
	return applied, nil
}

// compactTo discards log entries before index from memory and releases the
// WAL segments that only contain older entries.
func (n *Node) compactTo(index raft.Index) error {
	first, err := n.raftStorage.FirstIndex()
	if err != nil {
		return fmt.Errorf("node: first index: %w", err)
	}
	if index <= first {
		return nil
	}
	if err := n.raftStorage.Compact(index); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return fmt.Errorf("node: compact storage to %d: %w", index, err)
	}
	if err := n.wal.ReleaseBefore(index); err != nil {
		return fmt.Errorf("node: release wal before %d: %w", index, err)
	}
	stats := n.wal.Stats()
	n.metrics.WALSegments.Set(float64(stats.Segments))
	return nil
}
