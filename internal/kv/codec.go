package kv

import (
	"encoding/binary"
	"fmt"
)

// reader decodes fixed-width big-endian fields from untrusted input. Every
// method checks bounds and reports a short read as an error wrapping base, so
// decoders never panic on truncated or malicious data.
type reader struct {
	buf  []byte
	off  int
	base error
}

func (r *reader) remaining() int {
	return len(r.buf) - r.off
}

func (r *reader) need(n int) error {
	if n < 0 || r.remaining() < n {
		return fmt.Errorf("%w: unexpected end of data at offset %d", r.base, r.off)
	}
	return nil
}

func (r *reader) u8() (byte, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.buf[r.off]
	r.off++
	return v, nil
}

func (r *reader) u32() (uint32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v, nil
}

func (r *reader) u64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(r.buf[r.off:])
	r.off += 8
	return v, nil
}

// take returns the next n bytes without copying them.
func (r *reader) take(n int) ([]byte, error) {
	if err := r.need(n); err != nil {
		return nil, err
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

// lenPrefixed reads a u32 length followed by that many bytes.
func (r *reader) lenPrefixed() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	return r.take(int(n))
}

// appendLenPrefixed appends a u32 length followed by b.
func appendLenPrefixed(buf, b []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}
