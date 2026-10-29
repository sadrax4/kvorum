package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func commandsEqual(a, b Command) bool {
	return a.Op == b.Op && bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) &&
		a.ExpectedVersion == b.ExpectedVersion && a.ClientID == b.ClientID &&
		a.Sequence == b.Sequence && a.PrevKV == b.PrevKV
}

func TestCommandMarshalRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cmd  Command
	}{
		{name: "put", cmd: put("key", "value")},
		{name: "put empty value", cmd: put("key", "")},
		{name: "put with session and prev kv", cmd: withPrevKV(withSession(put("k", "v"), 42, 7))},
		{name: "delete", cmd: withSession(del("key"), 1, 1)},
		{name: "delete with prev kv", cmd: withPrevKV(del("key"))},
		{name: "compare and swap", cmd: withSession(cas("key", "value", 12), 9, 3)},
		{name: "max sizes", cmd: Command{Op: OpPut, Key: bytes.Repeat([]byte{0xff}, MaxKeySize), Value: bytes.Repeat([]byte{1}, MaxValueSize)}},
		{name: "max integers", cmd: Command{Op: OpCompareAndSwap, Key: []byte{0}, ExpectedVersion: ^uint64(0), ClientID: ^uint64(0), Sequence: ^uint64(0)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := tc.cmd.Marshal()
			if len(data) != commandHeaderSize+len(tc.cmd.Key)+len(tc.cmd.Value) {
				t.Fatalf("encoded %d bytes, want %d", len(data), commandHeaderSize+len(tc.cmd.Key)+len(tc.cmd.Value))
			}
			got, err := UnmarshalCommand(data)
			if err != nil {
				t.Fatalf("UnmarshalCommand: %v", err)
			}
			if !commandsEqual(got, tc.cmd) {
				t.Fatalf("round trip changed the command:\n got %+v\nwant %+v", got, tc.cmd)
			}
			if again := got.Marshal(); !bytes.Equal(again, data) {
				t.Fatal("re-encoding a decoded command produced different bytes")
			}
		})
	}
}

func TestCommandMarshalLayout(t *testing.T) {
	t.Parallel()
	cmd := Command{Op: OpCompareAndSwap, Key: []byte("k"), Value: []byte("v"), ExpectedVersion: 3, ClientID: 1, Sequence: 2, PrevKV: true}
	want := []byte{
		1,                      // format
		3,                      // op
		0x01,                   // flags: prev kv
		0, 0, 0, 0, 0, 0, 0, 1, // client id
		0, 0, 0, 0, 0, 0, 0, 2, // sequence
		0, 0, 0, 0, 0, 0, 0, 3, // expected version
		0, 0, 0, 1, // key length
		0, 0, 0, 1, // value length
		'k', 'v',
	}
	if got := cmd.Marshal(); !bytes.Equal(got, want) {
		t.Fatalf("Marshal() = % x\nwant        % x", got, want)
	}
}

func TestUnmarshalCommandCopiesInput(t *testing.T) {
	t.Parallel()
	data := put("key", "value").Marshal()
	cmd, err := UnmarshalCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = 0xff
	}
	if string(cmd.Key) != "key" || string(cmd.Value) != "value" {
		t.Fatalf("decoded command aliases its input: key %q value %q", cmd.Key, cmd.Value)
	}
}

// patchHeader returns the encoding of cmd with fn applied to its bytes.
func patchHeader(cmd Command, fn func(b []byte) []byte) []byte {
	return fn(cmd.Marshal())
}

func TestUnmarshalCommandRejects(t *testing.T) {
	t.Parallel()
	base := withSession(put("key", "value"), 5, 6)
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty", data: nil, want: ErrMalformedCommand},
		{name: "short header", data: base.Marshal()[:commandHeaderSize-1], want: ErrMalformedCommand},
		{name: "format zero", data: patchHeader(base, func(b []byte) []byte { b[0] = 0; return b }), want: ErrUnsupportedFormat},
		{name: "format from the future", data: patchHeader(base, func(b []byte) []byte { b[0] = 2; return b }), want: ErrUnsupportedFormat},
		{name: "unknown flag", data: patchHeader(base, func(b []byte) []byte { b[2] = 0x02; return b }), want: ErrMalformedCommand},
		{name: "high flag bit", data: patchHeader(base, func(b []byte) []byte { b[2] |= 0x80; return b }), want: ErrMalformedCommand},
		{name: "op zero", data: patchHeader(base, func(b []byte) []byte { b[1] = 0; return b }), want: ErrUnknownOp},
		{name: "op four", data: patchHeader(base, func(b []byte) []byte { b[1] = 4; return b }), want: ErrUnknownOp},
		{name: "op 255", data: patchHeader(base, func(b []byte) []byte { b[1] = 255; return b }), want: ErrUnknownOp},
		{name: "empty key", data: Command{Op: OpPut, Value: []byte("v")}.Marshal(), want: ErrEmptyKey},
		{name: "key too large", data: Command{Op: OpPut, Key: make([]byte, MaxKeySize+1)}.Marshal(), want: ErrKeyTooLarge},
		{name: "value too large", data: Command{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueSize+1)}.Marshal(), want: ErrValueTooLarge},
		{name: "truncated payload", data: base.Marshal()[:commandHeaderSize+3], want: ErrMalformedCommand},
		{name: "trailing byte", data: append(base.Marshal(), 0), want: ErrMalformedCommand},
		{name: "key length overflows payload", data: patchHeader(base, func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[27:31], 0xffffffff)
			return b
		}), want: ErrMalformedCommand},
		{name: "lengths sum past the buffer", data: patchHeader(base, func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[27:31], 0xffffffff)
			binary.BigEndian.PutUint32(b[31:35], 0xffffffff)
			return b
		}), want: ErrMalformedCommand},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd, err := UnmarshalCommand(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("UnmarshalCommand error = %v, want %v", err, tc.want)
			}
			if !errors.Is(tc.want, ErrUnsupportedFormat) && !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("error %v does not wrap ErrInvalidCommand", err)
			}
			if cmd.Key != nil || cmd.Value != nil {
				t.Fatalf("rejected input still produced a command: %+v", cmd)
			}
		})
	}
}

func TestCommandValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cmd  Command
		want error
	}{
		{name: "put", cmd: put("k", "v")},
		{name: "delete without value", cmd: del("k")},
		{name: "cas", cmd: cas("k", "v", 1)},
		{name: "largest key", cmd: Command{Op: OpPut, Key: make([]byte, MaxKeySize)}},
		{name: "largest value", cmd: Command{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueSize)}},
		{name: "zero op", cmd: Command{Key: []byte("k")}, want: ErrUnknownOp},
		{name: "unknown op", cmd: Command{Op: 9, Key: []byte("k")}, want: ErrUnknownOp},
		{name: "empty key", cmd: Command{Op: OpPut}, want: ErrEmptyKey},
		{name: "oversized key", cmd: Command{Op: OpPut, Key: make([]byte, MaxKeySize+1)}, want: ErrKeyTooLarge},
		{name: "oversized value", cmd: Command{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueSize+1)}, want: ErrValueTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cmd.Validate()
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want %v", err, tc.want)
			}
			if tc.want != nil && !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("error %v does not wrap ErrInvalidCommand", err)
			}
		})
	}
}

func TestOpTypeString(t *testing.T) {
	t.Parallel()
	tests := map[OpType]string{OpPut: "put", OpDelete: "delete", OpCompareAndSwap: "compare-and-swap", 0: "unknown(0)", 200: "unknown(200)"}
	for op, want := range tests {
		if got := op.String(); got != want {
			t.Errorf("OpType(%d).String() = %q, want %q", uint8(op), got, want)
		}
	}
}

func TestEventTypeString(t *testing.T) {
	t.Parallel()
	tests := map[EventType]string{EventPut: "put", EventDelete: "delete", 0: "unknown(0)", 7: "unknown(7)"}
	for typ, want := range tests {
		if got := typ.String(); got != want {
			t.Errorf("EventType(%d).String() = %q, want %q", uint8(typ), got, want)
		}
	}
}

func FuzzUnmarshalCommand(f *testing.F) {
	seeds := []Command{
		put("key", "value"),
		put("k", ""),
		withPrevKV(withSession(put("key", "value"), 1, 2)),
		withSession(del("key"), 3, 4),
		withPrevKV(cas("key", "new", 5)),
		cas("key", "new", 0),
	}
	for _, c := range seeds {
		f.Add(c.Marshal())
	}
	f.Add([]byte{})
	f.Add(make([]byte, commandHeaderSize))
	f.Add(bytes.Repeat([]byte{0xff}, commandHeaderSize+8))

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := UnmarshalCommand(data)
		if err != nil {
			if !errors.Is(err, ErrInvalidCommand) && !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("unexpected error class: %v", err)
			}
			if cmd.Key != nil || cmd.Value != nil {
				t.Fatalf("error %v returned alongside a non-empty command", err)
			}
			return
		}
		if err := cmd.Validate(); err != nil {
			t.Fatalf("decoded command fails validation: %v", err)
		}
		encoded := cmd.Marshal()
		if !bytes.Equal(encoded, data) {
			t.Fatalf("decode/encode is not the identity:\n in  % x\n out % x", data, encoded)
		}
		again, err := UnmarshalCommand(encoded)
		if err != nil {
			t.Fatalf("re-decoding failed: %v", err)
		}
		if !commandsEqual(cmd, again) {
			t.Fatalf("re-decoding changed the command: %+v vs %+v", cmd, again)
		}
	})
}
