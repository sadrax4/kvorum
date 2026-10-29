package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/sadrax4/kvorum/internal/kv/ordered"
)

// Snapshot layout, all integers big-endian:
//
//	magic "KVSTORE" | format u8 (1) | revision u64 |
//	key count u64 | key record* |
//	session count u64 | session record* |
//	crc32c u32 over everything before it
//
//	key record     := key length u32 | key | value length u32 | value |
//	                  version u64 | create revision u64 | mod revision u64
//	session record := client id u64 | sequence u64 | revision u64 | result
//	result         := flags u8 | [key record if flag kv] | [key record if flag prev kv]
//
// Keys are written in ascending order and sessions in ascending client id
// order, so identical state always produces identical bytes.
const (
	snapshotMagic         = "KVSTORE"
	snapshotFormatVersion = 1
	snapshotHeaderSize    = len(snapshotMagic) + 1
	snapshotTrailerSize   = 4

	resultFlagSucceeded byte = 1 << 0
	resultFlagDeleted   byte = 1 << 1
	resultFlagKV        byte = 1 << 2
	resultFlagPrevKV    byte = 1 << 3
	resultKnownFlags         = resultFlagSucceeded | resultFlagDeleted | resultFlagKV | resultFlagPrevKV

	// keyRecordMetaSize is the fixed part of a key record: two length prefixes
	// and three u64 fields. The smallest record also has a one-byte key.
	keyRecordMetaSize = 4 + 4 + 8 + 8 + 8
	minKeyRecordSize  = keyRecordMetaSize + 1
	// minSessionRecordSize is a session record whose result has no key values.
	minSessionRecordSize = 8 + 8 + 8 + 1
)

func crcTable() *crc32.Table {
	return crc32.MakeTable(crc32.Castagnoli)
}

// Snapshot serializes the revision, every key with its metadata, and the
// session table. The output is deterministic for identical state and
// protected by a CRC-32C trailer. Watchers are not included.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessions := s.sessions.sorted()
	buf := make([]byte, 0, s.snapshotSizeLocked(sessions))
	buf = append(buf, snapshotMagic...)
	buf = append(buf, snapshotFormatVersion)
	buf = binary.BigEndian.AppendUint64(buf, s.revision)

	buf = binary.BigEndian.AppendUint64(buf, uint64(s.index.Len()))
	s.index.AscendAll(func(key []byte, e *entry) bool {
		buf = appendKeyRecord(buf, key, e.value, e.version, e.createRevision, e.modRevision)
		return true
	})

	buf = binary.BigEndian.AppendUint64(buf, uint64(len(sessions)))
	for _, sess := range sessions {
		buf = binary.BigEndian.AppendUint64(buf, sess.clientID)
		buf = binary.BigEndian.AppendUint64(buf, sess.sequence)
		buf = binary.BigEndian.AppendUint64(buf, sess.revision)
		buf = appendResult(buf, sess.result)
	}

	buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable()))
	return buf, nil
}

// snapshotSizeLocked computes the exact encoded size so that Snapshot
// allocates once.
func (s *Store) snapshotSizeLocked(sessions []*session) int {
	size := snapshotHeaderSize + 8 + 8 + 8 + snapshotTrailerSize
	s.index.AscendAll(func(key []byte, e *entry) bool {
		size += keyRecordMetaSize + len(key) + len(e.value)
		return true
	})
	for _, sess := range sessions {
		size += minSessionRecordSize + keyValueSize(sess.result.KV) + keyValueSize(sess.result.PrevKV)
	}
	return size
}

func keyValueSize(kv *KeyValue) int {
	if kv == nil {
		return 0
	}
	return keyRecordMetaSize + len(kv.Key) + len(kv.Value)
}

func appendKeyRecord(buf, key, value []byte, version, createRevision, modRevision uint64) []byte {
	buf = appendLenPrefixed(buf, key)
	buf = appendLenPrefixed(buf, value)
	buf = binary.BigEndian.AppendUint64(buf, version)
	buf = binary.BigEndian.AppendUint64(buf, createRevision)
	buf = binary.BigEndian.AppendUint64(buf, modRevision)
	return buf
}

func appendResult(buf []byte, res Result) []byte {
	var flags byte
	if res.Succeeded {
		flags |= resultFlagSucceeded
	}
	if res.Deleted {
		flags |= resultFlagDeleted
	}
	if res.KV != nil {
		flags |= resultFlagKV
	}
	if res.PrevKV != nil {
		flags |= resultFlagPrevKV
	}
	buf = append(buf, flags)
	if res.KV != nil {
		buf = appendKeyRecord(buf, res.KV.Key, res.KV.Value, res.KV.Version, res.KV.CreateRevision, res.KV.ModRevision)
	}
	if res.PrevKV != nil {
		buf = appendKeyRecord(buf, res.PrevKV.Key, res.PrevKV.Value, res.PrevKV.Version, res.PrevKV.CreateRevision, res.PrevKV.ModRevision)
	}
	return buf
}

// Restore replaces the store's revision, keys, and session table with the
// contents of a snapshot produced by Snapshot. The data is fully validated
// before any state changes, so a failed Restore leaves the store untouched.
// Active watchers are cancelled with ErrWatcherOverflow because the events
// between the previous state and the restored one cannot be delivered.
func (s *Store) Restore(data []byte) error {
	snap, err := decodeSnapshot(data, s.maxSessions)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision = snap.revision
	s.index = snap.index
	s.sessions = snap.sessions
	s.cancelAllWatchersLocked(ErrWatcherOverflow)
	s.logger.Info("restored snapshot",
		"revision", snap.revision, "keys", snap.index.Len(), "sessions", snap.sessions.len())
	return nil
}

// decodedSnapshot is the fully validated content of a snapshot, built outside
// the store lock and swapped in atomically.
type decodedSnapshot struct {
	revision uint64
	index    *ordered.Map[*entry]
	sessions *sessionTable
}

func decodeSnapshot(data []byte, maxSessions int) (*decodedSnapshot, error) {
	if len(data) < snapshotHeaderSize+8+8+8+snapshotTrailerSize {
		return nil, fmt.Errorf("%w: %d bytes is too short", ErrCorruptSnapshot, len(data))
	}
	if string(data[:len(snapshotMagic)]) != snapshotMagic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorruptSnapshot)
	}
	if v := data[len(snapshotMagic)]; v != snapshotFormatVersion {
		return nil, fmt.Errorf("%w: snapshot format %d", ErrUnsupportedFormat, v)
	}
	body, trailer := data[:len(data)-snapshotTrailerSize], data[len(data)-snapshotTrailerSize:]
	if want, got := binary.BigEndian.Uint32(trailer), crc32.Checksum(body, crcTable()); got != want {
		return nil, fmt.Errorf("%w: checksum %#08x does not match recorded %#08x", ErrCorruptSnapshot, got, want)
	}

	r := &reader{buf: body[snapshotHeaderSize:], base: ErrCorruptSnapshot}
	revision, err := r.u64()
	if err != nil {
		return nil, err
	}
	index, err := decodeKeys(r, revision)
	if err != nil {
		return nil, err
	}
	sessions, err := decodeSessions(r, revision, maxSessions)
	if err != nil {
		return nil, err
	}
	if n := r.remaining(); n != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorruptSnapshot, n)
	}
	return &decodedSnapshot{revision: revision, index: index, sessions: sessions}, nil
}

func decodeKeys(r *reader, revision uint64) (*ordered.Map[*entry], error) {
	count, err := r.u64()
	if err != nil {
		return nil, err
	}
	if count > uint64(r.remaining()/minKeyRecordSize) {
		return nil, fmt.Errorf("%w: key count %d exceeds the payload", ErrCorruptSnapshot, count)
	}
	index := newIndex()
	var prevKey []byte
	for i := range count {
		kv, err := decodeKeyRecord(r, revision)
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		if i > 0 && bytes.Compare(prevKey, kv.Key) >= 0 {
			return nil, fmt.Errorf("%w: key %d is not in ascending order", ErrCorruptSnapshot, i)
		}
		prevKey = kv.Key
		index.Put(kv.Key, &entry{
			value:          kv.Value,
			version:        kv.Version,
			createRevision: kv.CreateRevision,
			modRevision:    kv.ModRevision,
		})
	}
	return index, nil
}

func decodeSessions(r *reader, revision uint64, maxSessions int) (*sessionTable, error) {
	count, err := r.u64()
	if err != nil {
		return nil, err
	}
	if count > uint64(r.remaining()/minSessionRecordSize) {
		return nil, fmt.Errorf("%w: session count %d exceeds the payload", ErrCorruptSnapshot, count)
	}
	sessions := make([]*session, 0, count)
	var prevID uint64
	for i := range count {
		sess, err := decodeSession(r, revision)
		if err != nil {
			return nil, fmt.Errorf("session %d: %w", i, err)
		}
		if i > 0 && sess.clientID <= prevID {
			return nil, fmt.Errorf("%w: session %d is not in ascending client id order", ErrCorruptSnapshot, i)
		}
		prevID = sess.clientID
		sessions = append(sessions, sess)
	}
	table := newSessionTable(maxSessions)
	table.restore(sessions)
	return table, nil
}

func decodeSession(r *reader, revision uint64) (*session, error) {
	clientID, err := r.u64()
	if err != nil {
		return nil, err
	}
	if clientID == 0 {
		return nil, fmt.Errorf("%w: zero client id", ErrCorruptSnapshot)
	}
	sequence, err := r.u64()
	if err != nil {
		return nil, err
	}
	touched, err := r.u64()
	if err != nil {
		return nil, err
	}
	if touched > revision {
		return nil, fmt.Errorf("%w: session revision %d exceeds snapshot revision %d", ErrCorruptSnapshot, touched, revision)
	}
	res, err := decodeResult(r, revision)
	if err != nil {
		return nil, err
	}
	return &session{clientID: clientID, sequence: sequence, revision: touched, result: res}, nil
}

func decodeResult(r *reader, revision uint64) (Result, error) {
	flags, err := r.u8()
	if err != nil {
		return Result{}, err
	}
	if flags&^resultKnownFlags != 0 {
		return Result{}, fmt.Errorf("%w: unknown result flags %#x", ErrCorruptSnapshot, flags)
	}
	res := Result{
		Succeeded: flags&resultFlagSucceeded != 0,
		Deleted:   flags&resultFlagDeleted != 0,
	}
	if flags&resultFlagKV != 0 {
		kv, err := decodeKeyRecord(r, revision)
		if err != nil {
			return Result{}, err
		}
		res.KV = &kv
	}
	if flags&resultFlagPrevKV != 0 {
		kv, err := decodeKeyRecord(r, revision)
		if err != nil {
			return Result{}, err
		}
		res.PrevKV = &kv
	}
	return res, nil
}

// decodeKeyRecord reads one key record and checks the invariants every stored
// key satisfies. The returned KeyValue owns its slices.
func decodeKeyRecord(r *reader, revision uint64) (KeyValue, error) {
	key, err := r.lenPrefixed()
	if err != nil {
		return KeyValue{}, err
	}
	value, err := r.lenPrefixed()
	if err != nil {
		return KeyValue{}, err
	}
	version, err := r.u64()
	if err != nil {
		return KeyValue{}, err
	}
	createRevision, err := r.u64()
	if err != nil {
		return KeyValue{}, err
	}
	modRevision, err := r.u64()
	if err != nil {
		return KeyValue{}, err
	}
	switch {
	case len(key) == 0:
		return KeyValue{}, fmt.Errorf("%w: empty key", ErrCorruptSnapshot)
	case len(key) > MaxKeySize:
		return KeyValue{}, fmt.Errorf("%w: key of %d bytes exceeds %d", ErrCorruptSnapshot, len(key), MaxKeySize)
	case len(value) > MaxValueSize:
		return KeyValue{}, fmt.Errorf("%w: value of %d bytes exceeds %d", ErrCorruptSnapshot, len(value), MaxValueSize)
	case version == 0:
		return KeyValue{}, fmt.Errorf("%w: key %q has version 0", ErrCorruptSnapshot, key)
	case createRevision == 0 || createRevision > modRevision || modRevision > revision:
		return KeyValue{}, fmt.Errorf("%w: key %q has revisions create=%d mod=%d under snapshot revision %d",
			ErrCorruptSnapshot, key, createRevision, modRevision, revision)
	}
	return KeyValue{
		Key:            bytes.Clone(key),
		Value:          cloneValue(value),
		Version:        version,
		CreateRevision: createRevision,
		ModRevision:    modRevision,
	}, nil
}
