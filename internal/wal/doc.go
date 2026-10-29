// Package wal implements the durable storage of a kvorum node: a segmented
// write-ahead log for Raft entries and hard state, and a snapshot store.
//
// # Write-ahead log
//
// The log is a sequence of segment files named by a monotonically increasing
// sequence number. Every segment starts with a header record, followed by
// entry, hard-state, and snapshot-marker records:
//
//	record := length u32 | crc32c u32 | type u8 | payload
//
// Integers are little-endian and the checksum covers the type byte and the
// payload. A new segment is cut when the active one exceeds the configured
// size; it begins with the current hard state and the latest snapshot marker,
// so that older segments become self-contained and can be released once a
// snapshot covers their entries.
//
// Replay rebuilds the state from the segments in order. An entry whose index
// is at or below the last replayed index overwrites the suffix from that
// index (a new leader replaced conflicting entries); a snapshot marker drops
// every replayed entry at or below its index. A torn record at the tail of
// the newest segment is the signature of a crash during a write and is
// truncated; a bad record anywhere else is corruption and fails Open.
//
// # Snapshots
//
// Snapshots are stored one per file, written to a temporary name, fsynced,
// and renamed into place, so a snapshot file is either complete or absent.
package wal
