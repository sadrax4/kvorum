package kv

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

func benchKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "bench/key/%012d", i)
	}
	rng := rand.New(rand.NewPCG(3, 4))
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	return keys
}

func benchStore(b *testing.B, n int) (*Store, [][]byte) {
	b.Helper()
	s := NewStore(Options{})
	keys := benchKeys(n)
	value := bytes.Repeat([]byte("v"), 128)
	for i, k := range keys {
		if res := s.Apply(uint64(i+1), Command{Op: OpPut, Key: k, Value: value}); res.Err != nil {
			b.Fatal(res.Err)
		}
	}
	return s, keys
}

func BenchmarkApplyPut(b *testing.B) {
	const n = 1 << 16
	s := NewStore(Options{})
	keys := benchKeys(n)
	value := bytes.Repeat([]byte("v"), 128)
	b.ReportAllocs()
	rev := uint64(0)
	for i := 0; b.Loop(); i++ {
		rev++
		if res := s.Apply(rev, Command{Op: OpPut, Key: keys[i%n], Value: value}); res.Err != nil {
			b.Fatal(res.Err)
		}
	}
}

func BenchmarkApplyPutWithSession(b *testing.B) {
	const n = 1 << 16
	s := NewStore(Options{})
	keys := benchKeys(n)
	value := bytes.Repeat([]byte("v"), 128)
	b.ReportAllocs()
	rev := uint64(0)
	for i := 0; b.Loop(); i++ {
		rev++
		cmd := Command{Op: OpPut, Key: keys[i%n], Value: value, ClientID: uint64(i%1024 + 1), Sequence: rev}
		if res := s.Apply(rev, cmd); res.Err != nil {
			b.Fatal(res.Err)
		}
	}
}

func BenchmarkApplyPutWithWatcher(b *testing.B) {
	const n = 1 << 16
	s := NewStore(Options{})
	keys := benchKeys(n)
	value := bytes.Repeat([]byte("v"), 128)
	w := s.Watch(nil, []byte{0}, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range w.Events() {
		}
	}()
	b.ReportAllocs()
	rev := uint64(0)
	for i := 0; b.Loop(); i++ {
		rev++
		if res := s.Apply(rev, Command{Op: OpPut, Key: keys[i%n], Value: value}); res.Err != nil {
			b.Fatal(res.Err)
		}
	}
	b.StopTimer()
	w.Cancel()
	<-done
}

func BenchmarkGet(b *testing.B) {
	const n = 1 << 16
	s, keys := benchStore(b, n)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, ok := s.Get(keys[i%n]); !ok {
			b.Fatal("key missing")
		}
	}
}

func BenchmarkScan(b *testing.B) {
	const n = 1 << 16
	s, _ := benchStore(b, n)
	start := []byte("bench/key/000000030000")
	b.ReportAllocs()
	for b.Loop() {
		if kvs, _ := s.Scan(start, nil, 100); len(kvs) != 100 {
			b.Fatalf("scanned %d keys", len(kvs))
		}
	}
}

func BenchmarkSnapshot(b *testing.B) {
	const n = 1 << 14
	s, _ := benchStore(b, n)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Snapshot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRestore(b *testing.B) {
	const n = 1 << 14
	s, _ := benchStore(b, n)
	data, err := s.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	dst := NewStore(Options{})
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if err := dst.Restore(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommandMarshal(b *testing.B) {
	cmd := Command{Op: OpPut, Key: []byte("bench/key/000000000001"), Value: bytes.Repeat([]byte("v"), 128), ClientID: 7, Sequence: 9}
	b.ReportAllocs()
	for b.Loop() {
		_ = cmd.Marshal()
	}
}

func BenchmarkUnmarshalCommand(b *testing.B) {
	data := Command{Op: OpPut, Key: []byte("bench/key/000000000001"), Value: bytes.Repeat([]byte("v"), 128), ClientID: 7, Sequence: 9}.Marshal()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := UnmarshalCommand(data); err != nil {
			b.Fatal(err)
		}
	}
}
