package client

import (
	"bytes"
	"testing"
)

func TestPrefixEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{name: "empty", prefix: nil, want: nil},
		{name: "empty slice", prefix: []byte{}, want: nil},
		{name: "simple", prefix: []byte("a"), want: []byte("b")},
		{name: "multi byte", prefix: []byte("foo/"), want: []byte("foo0")},
		{name: "trailing ff dropped", prefix: []byte{'a', 0xff}, want: []byte{'b'}},
		{name: "several trailing ff", prefix: []byte{'a', 'b', 0xff, 0xff}, want: []byte{'a', 'c'}},
		{name: "all ff", prefix: []byte{0xff, 0xff}, want: nil},
		{name: "single ff", prefix: []byte{0xff}, want: nil},
		{name: "fe becomes ff", prefix: []byte{0xfe}, want: []byte{0xff}},
		{name: "zero byte", prefix: []byte{0x00}, want: []byte{0x01}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			orig := bytes.Clone(tc.prefix)
			got := PrefixEnd(tc.prefix)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("PrefixEnd(%q) = %q, want %q", tc.prefix, got, tc.want)
			}
			if !bytes.Equal(orig, tc.prefix) {
				t.Fatalf("PrefixEnd mutated its input: %q -> %q", orig, tc.prefix)
			}
		})
	}
}

func TestPrefixEndCoversPrefixRange(t *testing.T) {
	t.Parallel()
	prefix := []byte("user/")
	end := PrefixEnd(prefix)
	inside := [][]byte{[]byte("user/"), []byte("user/a"), []byte("user/\xff\xff")}
	outside := [][]byte{[]byte("user"), []byte("user0"), []byte("usf")}
	for _, k := range inside {
		if bytes.Compare(k, prefix) < 0 || bytes.Compare(k, end) >= 0 {
			t.Errorf("key %q should be inside [%q, %q)", k, prefix, end)
		}
	}
	for _, k := range outside {
		if bytes.Compare(k, prefix) >= 0 && bytes.Compare(k, end) < 0 {
			t.Errorf("key %q should be outside [%q, %q)", k, prefix, end)
		}
	}
}

func TestWatchRangeEnd(t *testing.T) {
	t.Parallel()
	if got := watchRangeEnd([]byte("app/"), watchOptions{prefix: true}); string(got) != "app0" {
		t.Fatalf("prefix range end = %q", got)
	}
	if got := watchRangeEnd([]byte{0xff}, watchOptions{prefix: true}); string(got) != "\x00" {
		t.Fatalf("all-0xff prefix must watch to the end of the keyspace, got %q", got)
	}
	if got := watchRangeEnd([]byte("k"), watchOptions{rangeEnd: []byte("z")}); string(got) != "z" {
		t.Fatalf("explicit range end = %q", got)
	}
	if got := watchRangeEnd([]byte("k"), watchOptions{}); got != nil {
		t.Fatalf("single key watch should have no range end, got %q", got)
	}
}
