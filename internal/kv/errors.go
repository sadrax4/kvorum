package kv

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidCommand is the family of errors for commands the store refuses
	// to apply. The specific errors below wrap it, so callers may match either
	// the family or the exact cause with errors.Is.
	ErrInvalidCommand = errors.New("kv: invalid command")
	// ErrUnknownOp wraps ErrInvalidCommand for an unrecognized operation type.
	ErrUnknownOp = fmt.Errorf("%w: unknown operation", ErrInvalidCommand)
	// ErrEmptyKey wraps ErrInvalidCommand for a command whose key is empty.
	ErrEmptyKey = fmt.Errorf("%w: empty key", ErrInvalidCommand)
	// ErrKeyTooLarge wraps ErrInvalidCommand for a key longer than MaxKeySize.
	ErrKeyTooLarge = fmt.Errorf("%w: key exceeds %d bytes", ErrInvalidCommand, MaxKeySize)
	// ErrValueTooLarge wraps ErrInvalidCommand for a value longer than
	// MaxValueSize.
	ErrValueTooLarge = fmt.Errorf("%w: value exceeds %d bytes", ErrInvalidCommand, MaxValueSize)
	// ErrMalformedCommand wraps ErrInvalidCommand for bytes that do not decode
	// as a command.
	ErrMalformedCommand = fmt.Errorf("%w: malformed encoding", ErrInvalidCommand)

	// ErrUnsupportedFormat is returned when encoded data carries a format
	// version this build does not understand.
	ErrUnsupportedFormat = errors.New("kv: unsupported format version")

	// ErrStaleSequence is returned by Apply when a command's sequence number is
	// lower than the last one recorded for its client, which means a newer
	// command from the same client has already been applied.
	ErrStaleSequence = errors.New("kv: stale sequence number")

	// ErrRevisionNotMonotonic is returned by Apply when the revision is not
	// strictly greater than the revision of the previous command.
	ErrRevisionNotMonotonic = errors.New("kv: revision is not greater than the last applied revision")

	// ErrWatcherOverflow is reported by Watcher.Err when the store cancelled
	// the watcher because its events could no longer be delivered: the channel
	// was full when an event arrived, or the state was replaced by a snapshot.
	ErrWatcherOverflow = errors.New("kv: watcher fell behind and was cancelled")

	// ErrWatcherCancelled is reported by Watcher.Err after Watcher.Cancel.
	ErrWatcherCancelled = errors.New("kv: watcher cancelled")

	// ErrCorruptSnapshot is returned by Restore when the data fails validation:
	// bad magic, checksum mismatch, truncation, or inconsistent contents.
	ErrCorruptSnapshot = errors.New("kv: corrupt snapshot")
)
