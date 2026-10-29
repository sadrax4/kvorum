package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	// MaxKeySize is the maximum length of a key in bytes.
	MaxKeySize = 4096
	// MaxValueSize is the maximum length of a value in bytes.
	MaxValueSize = 4 << 20
)

// OpType identifies the operation a Command performs.
type OpType uint8

const (
	// OpPut stores Value under Key, creating the key or overwriting it.
	OpPut OpType = 1
	// OpDelete removes Key.
	OpDelete OpType = 2
	// OpCompareAndSwap stores Value under Key only if the key's current version
	// equals ExpectedVersion. An ExpectedVersion of zero requires the key to be
	// absent, which makes the operation a create-if-absent primitive.
	OpCompareAndSwap OpType = 3
)

// String implements fmt.Stringer.
func (op OpType) String() string {
	switch op {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCompareAndSwap:
		return "compare-and-swap"
	default:
		return "unknown(" + strconv.Itoa(int(op)) + ")"
	}
}

// Command is a single state-machine operation. It is what the server
// proposes to Raft and what Store.Apply executes once the entry commits.
type Command struct {
	// Op selects the operation.
	Op OpType
	// Key is the key to operate on. It must be non-empty and at most
	// MaxKeySize bytes.
	Key []byte
	// Value is the value to store for OpPut and OpCompareAndSwap. It must be at
	// most MaxValueSize bytes and is ignored by OpDelete.
	Value []byte
	// ExpectedVersion is the version the key must have for OpCompareAndSwap to
	// succeed; zero means the key must not exist.
	ExpectedVersion uint64
	// ClientID identifies the client session for exactly-once semantics. Zero
	// disables deduplication for this command.
	ClientID uint64
	// Sequence is the client's per-session command counter. It must increase
	// with every new command and be repeated verbatim on retries.
	Sequence uint64
	// PrevKV asks for the key's previous value in Result.PrevKV.
	PrevKV bool
}

// Binary layout of a command, all integers big-endian:
//
//	format u8 (1) | op u8 | flags u8 | client id u64 | sequence u64 |
//	expected version u64 | key length u32 | value length u32 | key | value
const (
	commandFormatVersion = 1
	commandHeaderSize    = 1 + 1 + 1 + 8 + 8 + 8 + 4 + 4

	commandFlagPrevKV byte = 1 << 0
	commandKnownFlags      = commandFlagPrevKV
)

// Validate reports whether the command can be applied: the operation must be
// known, the key non-empty and within MaxKeySize, and the value within
// MaxValueSize. The returned error wraps ErrInvalidCommand.
func (c Command) Validate() error {
	switch c.Op {
	case OpPut, OpDelete, OpCompareAndSwap:
	default:
		return fmt.Errorf("%w %d", ErrUnknownOp, c.Op)
	}
	switch {
	case len(c.Key) == 0:
		return ErrEmptyKey
	case len(c.Key) > MaxKeySize:
		return fmt.Errorf("%w (got %d)", ErrKeyTooLarge, len(c.Key))
	case len(c.Value) > MaxValueSize:
		return fmt.Errorf("%w (got %d)", ErrValueTooLarge, len(c.Value))
	}
	return nil
}

// Marshal encodes the command in the versioned binary format understood by
// UnmarshalCommand. It performs no validation; call Validate before proposing
// a command built from client input.
func (c Command) Marshal() []byte {
	var flags byte
	if c.PrevKV {
		flags |= commandFlagPrevKV
	}
	buf := make([]byte, 0, commandHeaderSize+len(c.Key)+len(c.Value))
	buf = append(buf, commandFormatVersion, byte(c.Op), flags)
	buf = binary.BigEndian.AppendUint64(buf, c.ClientID)
	buf = binary.BigEndian.AppendUint64(buf, c.Sequence)
	buf = binary.BigEndian.AppendUint64(buf, c.ExpectedVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Key)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Value)))
	buf = append(buf, c.Key...)
	buf = append(buf, c.Value...)
	return buf
}

// UnmarshalCommand decodes a command produced by Command.Marshal. It rejects
// unknown format versions with ErrUnsupportedFormat and anything else that is
// not a well-formed, valid command with an error wrapping ErrInvalidCommand:
// truncated or oversized input, unknown flags, unknown operations, empty or
// oversized keys, and oversized values. The returned command owns its slices.
func UnmarshalCommand(b []byte) (Command, error) {
	if len(b) < commandHeaderSize {
		return Command{}, fmt.Errorf("%w: %d bytes is shorter than the %d-byte header", ErrMalformedCommand, len(b), commandHeaderSize)
	}
	if v := b[0]; v != commandFormatVersion {
		return Command{}, fmt.Errorf("%w: command format %d", ErrUnsupportedFormat, v)
	}
	flags := b[2]
	if flags&^commandKnownFlags != 0 {
		return Command{}, fmt.Errorf("%w: unknown flags %#x", ErrMalformedCommand, flags)
	}
	keyLen := uint64(binary.BigEndian.Uint32(b[27:31]))
	valueLen := uint64(binary.BigEndian.Uint32(b[31:35]))
	if want := commandHeaderSize + keyLen + valueLen; uint64(len(b)) != want {
		return Command{}, fmt.Errorf("%w: %d bytes but the header declares %d", ErrMalformedCommand, len(b), want)
	}
	payload := b[commandHeaderSize:]
	cmd := Command{
		Op:              OpType(b[1]),
		Key:             payload[:keyLen],
		Value:           payload[keyLen:],
		ExpectedVersion: binary.BigEndian.Uint64(b[19:27]),
		ClientID:        binary.BigEndian.Uint64(b[3:11]),
		Sequence:        binary.BigEndian.Uint64(b[11:19]),
		PrevKV:          flags&commandFlagPrevKV != 0,
	}
	if err := cmd.Validate(); err != nil {
		return Command{}, err
	}
	cmd.Key = bytes.Clone(cmd.Key)
	cmd.Value = cloneValue(cmd.Value)
	return cmd, nil
}
