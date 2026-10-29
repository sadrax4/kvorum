package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/sadrax4/kvorum/raft"
)

// Record types.
const (
	recordEntry     byte = 0x01
	recordHardState byte = 0x02
	recordSnapshot  byte = 0x03
	recordHeader    byte = 0x7F
)

const (
	recordPrefixSize = 4 + 4 + 1 // length, crc, type
	// maxRecordSize bounds the length prefix so that a corrupt prefix cannot
	// trigger a huge allocation.
	maxRecordSize = 256 << 20
)

var (
	headerMagic = []byte("KVWAL")

	formatVersion byte = 1

	crcTable = crc32.MakeTable(crc32.Castagnoli)

	errBadChecksum = errors.New("checksum mismatch")
	errTornRecord  = errors.New("torn record")
)

// appendRecord encodes a record into buf.
func appendRecord(buf []byte, typ byte, payload []byte) []byte {
	// length counts the type byte and the payload.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(1+len(payload)))
	crc := crc32.Update(0, crcTable, []byte{typ})
	crc = crc32.Update(crc, crcTable, payload)
	buf = binary.LittleEndian.AppendUint32(buf, crc)
	buf = append(buf, typ)
	buf = append(buf, payload...)
	return buf
}

// readRecord reads one record. It returns io.EOF at a clean end of file,
// errTornRecord (wrapping the underlying cause) if the file ends inside a
// record, and errBadChecksum if the record is complete but corrupt.
func readRecord(r *bufio.Reader) (typ byte, payload []byte, err error) {
	var prefix [8]byte
	n, err := io.ReadFull(r, prefix[:])
	if err != nil {
		if n == 0 && errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("%w: short prefix: %w", errTornRecord, err)
	}
	length := binary.LittleEndian.Uint32(prefix[:4])
	crc := binary.LittleEndian.Uint32(prefix[4:8])
	if length == 0 || length > maxRecordSize {
		return 0, nil, fmt.Errorf("%w: implausible record length %d", errBadChecksum, length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, fmt.Errorf("%w: short body: %w", errTornRecord, err)
	}
	if crc32.Checksum(body, crcTable) != crc {
		return 0, nil, errBadChecksum
	}
	return body[0], body[1:], nil
}

func encodeHeader() []byte {
	payload := append([]byte(nil), headerMagic...)
	return append(payload, formatVersion)
}

func decodeHeader(payload []byte) error {
	if len(payload) != len(headerMagic)+1 || string(payload[:len(headerMagic)]) != string(headerMagic) {
		return errors.New("bad segment magic")
	}
	if v := payload[len(headerMagic)]; v != formatVersion {
		return fmt.Errorf("unsupported segment format version %d", v)
	}
	return nil
}

func encodeEntry(e raft.Entry) []byte {
	buf := make([]byte, 0, 17+len(e.Data))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Index))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Term))
	buf = append(buf, byte(e.Type))
	return append(buf, e.Data...)
}

func decodeEntry(payload []byte) (raft.Entry, error) {
	if len(payload) < 17 {
		return raft.Entry{}, errors.New("entry record too short")
	}
	e := raft.Entry{
		Index: raft.Index(binary.LittleEndian.Uint64(payload[0:8])),
		Term:  raft.Term(binary.LittleEndian.Uint64(payload[8:16])),
		Type:  raft.EntryType(payload[16]),
	}
	if e.Type != raft.EntryNormal && e.Type != raft.EntryConfChange {
		return raft.Entry{}, fmt.Errorf("unknown entry type %d", e.Type)
	}
	if len(payload) > 17 {
		e.Data = append([]byte(nil), payload[17:]...)
	}
	return e, nil
}

func encodeHardState(hs raft.HardState) []byte {
	buf := make([]byte, 0, 24)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hs.Term))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hs.Vote))
	return binary.LittleEndian.AppendUint64(buf, uint64(hs.Commit))
}

func decodeHardState(payload []byte) (raft.HardState, error) {
	if len(payload) != 24 {
		return raft.HardState{}, fmt.Errorf("hard state record has %d bytes, want 24", len(payload))
	}
	return raft.HardState{
		Term:   raft.Term(binary.LittleEndian.Uint64(payload[0:8])),
		Vote:   raft.NodeID(binary.LittleEndian.Uint64(payload[8:16])),
		Commit: raft.Index(binary.LittleEndian.Uint64(payload[16:24])),
	}, nil
}

func encodeSnapshotMeta(meta raft.SnapshotMeta) []byte {
	cs, err := meta.ConfState.MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("wal: marshal conf state: %v", err)) // cannot fail
	}
	buf := make([]byte, 0, 16+len(cs))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(meta.Index))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(meta.Term))
	return append(buf, cs...)
}

func decodeSnapshotMeta(payload []byte) (raft.SnapshotMeta, error) {
	if len(payload) < 16 {
		return raft.SnapshotMeta{}, errors.New("snapshot marker too short")
	}
	meta := raft.SnapshotMeta{
		Index: raft.Index(binary.LittleEndian.Uint64(payload[0:8])),
		Term:  raft.Term(binary.LittleEndian.Uint64(payload[8:16])),
	}
	if err := meta.ConfState.UnmarshalBinary(payload[16:]); err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("snapshot marker conf state: %w", err)
	}
	return meta, nil
}
