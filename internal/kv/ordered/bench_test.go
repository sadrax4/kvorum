package ordered

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func benchmarkKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%012d", i)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	return keys
}

func BenchmarkMapPut(b *testing.B) {
	const n = 1 << 16
	keys := benchmarkKeys(n)
	var m *Map[int]
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%n == 0 {
			m = New[int](uint64(i))
		}
		m.Put(keys[i%n], i)
	}
}

func BenchmarkMapPutReplace(b *testing.B) {
	const n = 1 << 16
	keys := benchmarkKeys(n)
	m := New[int](1)
	for i, k := range keys {
		m.Put(k, i)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		m.Put(keys[i%n], i)
	}
}

func BenchmarkMapGet(b *testing.B) {
	const n = 1 << 16
	keys := benchmarkKeys(n)
	m := New[int](1)
	for i, k := range keys {
		m.Put(k, i)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Get(keys[i%n]); !ok {
			b.Fatal("key missing")
		}
	}
}

func BenchmarkMapAscend(b *testing.B) {
	const n = 1 << 16
	keys := benchmarkKeys(n)
	m := New[int](1)
	for i, k := range keys {
		m.Put(k, i)
	}
	start := []byte("key-000000030000")
	b.ReportAllocs()
	for b.Loop() {
		count := 0
		m.Ascend(start, nil, func([]byte, int) bool {
			count++
			return count < 100
		})
	}
}
