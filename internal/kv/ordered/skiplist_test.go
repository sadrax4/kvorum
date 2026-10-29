package ordered

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// heights returns the tower height of every node in key order.
func heights[V any](m *Map[V]) []int {
	var out []int
	for n := m.head.next[0]; n != nil; n = n.next[0] {
		out = append(out, len(n.next))
	}
	return out
}

// checkInvariants verifies the structural properties every skip list must
// uphold: level 0 is strictly ascending and holds Len() keys, every higher
// level is exactly the subsequence of level-0 nodes tall enough to reach it,
// and m.level tracks the tallest tower.
func checkInvariants[V any](t *testing.T, m *Map[V]) {
	t.Helper()
	var level0 []*node[V]
	for n := m.head.next[0]; n != nil; n = n.next[0] {
		if len(n.next) < 1 || len(n.next) > maxLevel {
			t.Fatalf("node %q has height %d outside [1, %d]", n.key, len(n.next), maxLevel)
		}
		if len(level0) > 0 && bytes.Compare(level0[len(level0)-1].key, n.key) >= 0 {
			t.Fatalf("level 0 is not strictly ascending around %q", n.key)
		}
		level0 = append(level0, n)
	}
	if len(level0) != m.Len() {
		t.Fatalf("level 0 holds %d nodes, Len() = %d", len(level0), m.Len())
	}
	for lvl := 1; lvl < maxLevel; lvl++ {
		var want []*node[V]
		for _, n := range level0 {
			if len(n.next) > lvl {
				want = append(want, n)
			}
		}
		var got []*node[V]
		for n := m.head.next[lvl]; n != nil; n = n.next[lvl] {
			got = append(got, n)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("level %d chain has %d nodes, want %d", lvl, len(got), len(want))
		}
	}
	if m.level < 1 || m.level > maxLevel {
		t.Fatalf("m.level = %d outside [1, %d]", m.level, maxLevel)
	}
	for lvl := m.level; lvl < maxLevel; lvl++ {
		if m.head.next[lvl] != nil {
			t.Fatalf("head has a successor at level %d above m.level = %d", lvl, m.level)
		}
	}
	if m.Len() > 0 && m.head.next[m.level-1] == nil {
		t.Fatalf("m.level = %d but the top chain is empty", m.level)
	}
	if m.Len() == 0 && m.level != 1 {
		t.Fatalf("empty map has m.level = %d, want 1", m.level)
	}
}

func collect[V any](m *Map[V], start, end []byte) ([]string, []V) {
	var keys []string
	var values []V
	m.Ascend(start, end, func(key []byte, v V) bool {
		keys = append(keys, string(key))
		values = append(values, v)
		return true
	})
	return keys, values
}

func TestMapEmpty(t *testing.T) {
	t.Parallel()
	m := New[int](1)
	if m.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", m.Len())
	}
	if v, ok := m.Get([]byte("a")); ok || v != 0 {
		t.Fatalf("Get on empty map = (%d, %t), want (0, false)", v, ok)
	}
	if v, ok := m.Delete([]byte("a")); ok || v != 0 {
		t.Fatalf("Delete on empty map = (%d, %t), want (0, false)", v, ok)
	}
	calls := 0
	m.AscendAll(func([]byte, int) bool {
		calls++
		return true
	})
	if calls != 0 {
		t.Fatalf("AscendAll on empty map made %d calls", calls)
	}
	checkInvariants(t, m)
}

func TestMapPutGetDelete(t *testing.T) {
	t.Parallel()
	m := New[string](7)

	if prev, replaced := m.Put([]byte("b"), "B1"); replaced || prev != "" {
		t.Fatalf("first Put = (%q, %t), want (\"\", false)", prev, replaced)
	}
	if prev, replaced := m.Put([]byte("a"), "A1"); replaced || prev != "" {
		t.Fatalf("Put(a) = (%q, %t), want (\"\", false)", prev, replaced)
	}
	if prev, replaced := m.Put([]byte("c"), "C1"); replaced || prev != "" {
		t.Fatalf("Put(c) = (%q, %t), want (\"\", false)", prev, replaced)
	}
	if m.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", m.Len())
	}
	if prev, replaced := m.Put([]byte("b"), "B2"); !replaced || prev != "B1" {
		t.Fatalf("overwrite Put = (%q, %t), want (\"B1\", true)", prev, replaced)
	}
	if m.Len() != 3 {
		t.Fatalf("Len() after overwrite = %d, want 3", m.Len())
	}
	if v, ok := m.Get([]byte("b")); !ok || v != "B2" {
		t.Fatalf("Get(b) = (%q, %t), want (\"B2\", true)", v, ok)
	}
	if v, ok := m.Get([]byte("bb")); ok || v != "" {
		t.Fatalf("Get(bb) = (%q, %t), want (\"\", false)", v, ok)
	}
	if v, ok := m.Delete([]byte("b")); !ok || v != "B2" {
		t.Fatalf("Delete(b) = (%q, %t), want (\"B2\", true)", v, ok)
	}
	if v, ok := m.Delete([]byte("b")); ok || v != "" {
		t.Fatalf("second Delete(b) = (%q, %t), want (\"\", false)", v, ok)
	}
	if _, ok := m.Get([]byte("b")); ok {
		t.Fatal("Get(b) found a deleted key")
	}
	keys, values := collect(m, nil, nil)
	if !slices.Equal(keys, []string{"a", "c"}) || !slices.Equal(values, []string{"A1", "C1"}) {
		t.Fatalf("contents = %v %v, want [a c] [A1 C1]", keys, values)
	}
	checkInvariants(t, m)
}

func TestMapPutKeepsOriginalKeySlice(t *testing.T) {
	t.Parallel()
	m := New[int](3)
	original := []byte("key")
	m.Put(original, 1)
	m.Put([]byte("key"), 2)
	var seen []byte
	m.AscendAll(func(key []byte, _ int) bool {
		seen = key
		return true
	})
	if &seen[0] != &original[0] {
		t.Fatal("overwrite replaced the key slice held by the map")
	}
}

func TestMapEmptyKeySortsFirst(t *testing.T) {
	t.Parallel()
	m := New[int](9)
	m.Put([]byte("a"), 1)
	m.Put([]byte{}, 0)
	m.Put([]byte{0}, 2)
	keys, values := collect(m, nil, nil)
	if !slices.Equal(keys, []string{"", "\x00", "a"}) || !slices.Equal(values, []int{0, 2, 1}) {
		t.Fatalf("contents = %q %v", keys, values)
	}
	if v, ok := m.Get(nil); !ok || v != 0 {
		t.Fatalf("Get(nil) = (%d, %t), want (0, true)", v, ok)
	}
	if v, ok := m.Delete([]byte{}); !ok || v != 0 {
		t.Fatalf("Delete(empty) = (%d, %t), want (0, true)", v, ok)
	}
	checkInvariants(t, m)
}

func TestMapAscend(t *testing.T) {
	t.Parallel()
	m := New[int](11)
	for i, k := range []string{"a", "b", "c", "d", "e"} {
		m.Put([]byte(k), i)
	}
	tests := []struct {
		name       string
		start, end string
		stopAfter  int
		want       []string
	}{
		{name: "all", want: []string{"a", "b", "c", "d", "e"}},
		{name: "from start inclusive", start: "b", want: []string{"b", "c", "d", "e"}},
		{name: "to end exclusive", end: "c", want: []string{"a", "b"}},
		{name: "closed range", start: "b", end: "d", want: []string{"b", "c"}},
		{name: "start between keys", start: "bb", end: "dd", want: []string{"c", "d"}},
		{name: "start after last key", start: "f", want: nil},
		{name: "end before first key", end: "a", want: nil},
		{name: "empty range", start: "c", end: "c", want: nil},
		{name: "inverted range", start: "d", end: "b", want: nil},
		{name: "early stop", stopAfter: 2, want: []string{"a", "b"}},
		{name: "early stop within range", start: "b", end: "e", stopAfter: 1, want: []string{"b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			m.Ascend([]byte(tc.start), []byte(tc.end), func(key []byte, v int) bool {
				if v != int(key[0]-'a') {
					t.Fatalf("key %q carries value %d", key, v)
				}
				got = append(got, string(key))
				return tc.stopAfter == 0 || len(got) < tc.stopAfter
			})
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Ascend(%q, %q) = %v, want %v", tc.start, tc.end, got, tc.want)
			}
		})
	}
}

func TestMapSameSeedSameShape(t *testing.T) {
	t.Parallel()
	build := func(seed uint64) *Map[int] {
		m := New[int](seed)
		rng := rand.New(rand.NewPCG(99, 100))
		for i := range 2000 {
			k := fmt.Appendf(nil, "k%05d", rng.IntN(5000))
			switch rng.IntN(3) {
			case 0, 1:
				m.Put(k, i)
			default:
				m.Delete(k)
			}
		}
		return m
	}
	a, b := build(42), build(42)
	if !slices.Equal(heights(a), heights(b)) || a.level != b.level {
		t.Fatal("maps built with the same seed and operations differ in shape")
	}
	c := build(43)
	if slices.Equal(heights(a), heights(c)) {
		t.Fatal("maps built with different seeds have identical shape")
	}
	ka, _ := collect(a, nil, nil)
	kc, _ := collect(c, nil, nil)
	if !slices.Equal(ka, kc) {
		t.Fatal("seed changed the logical contents of the map")
	}
	checkInvariants(t, a)
	checkInvariants(t, c)
}

func TestMapHeightsAreBoundedAndGeometric(t *testing.T) {
	t.Parallel()
	m := New[struct{}](5)
	const n = 20000
	for i := range n {
		m.Put(fmt.Appendf(nil, "%08d", i), struct{}{})
	}
	counts := make(map[int]int)
	for _, h := range heights(m) {
		if h < 1 || h > maxLevel {
			t.Fatalf("height %d outside [1, %d]", h, maxLevel)
		}
		counts[h]++
	}
	// With p = 1/4 roughly 75% of the nodes have height 1 and 19% height 2.
	if frac := float64(counts[1]) / n; frac < 0.7 || frac > 0.8 {
		t.Fatalf("%.3f of nodes have height 1, want about 0.75", frac)
	}
	if frac := float64(counts[2]) / n; frac < 0.15 || frac > 0.23 {
		t.Fatalf("%.3f of nodes have height 2, want about 0.19", frac)
	}
	if m.level > 16 {
		t.Fatalf("m.level = %d is implausibly high for %d keys", m.level, n)
	}
	checkInvariants(t, m)
}

func TestMapLargeShuffledInsertIteratesSorted(t *testing.T) {
	t.Parallel()
	const n = 50000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "%06x", i)
	}
	rng := rand.New(rand.NewPCG(8, 9))
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	m := New[int](17)
	for i, k := range keys {
		m.Put(k, i)
	}
	if m.Len() != n {
		t.Fatalf("Len() = %d, want %d", m.Len(), n)
	}
	var prev []byte
	count := 0
	m.AscendAll(func(key []byte, v int) bool {
		if prev != nil && bytes.Compare(prev, key) >= 0 {
			t.Fatalf("iteration not ascending: %q then %q", prev, key)
		}
		if !bytes.Equal(keys[v], key) {
			t.Fatalf("key %q carries value %d which maps to %q", key, v, keys[v])
		}
		prev = key
		count++
		return true
	})
	if count != n {
		t.Fatalf("iterated %d keys, want %d", count, n)
	}
	for i := 0; i < n; i += 2 {
		if _, ok := m.Delete(keys[i]); !ok {
			t.Fatalf("Delete(%q) found nothing", keys[i])
		}
	}
	if m.Len() != n/2 {
		t.Fatalf("Len() after deletes = %d, want %d", m.Len(), n/2)
	}
	checkInvariants(t, m)
}

func TestMapGenericValueTypes(t *testing.T) {
	t.Parallel()
	type record struct {
		n int
		s string
	}
	ptrs := New[*record](1)
	r := &record{n: 1, s: "one"}
	ptrs.Put([]byte("r"), r)
	if got, ok := ptrs.Get([]byte("r")); !ok || got != r {
		t.Fatal("pointer value round trip failed")
	}
	if got, ok := ptrs.Get([]byte("missing")); ok || got != nil {
		t.Fatal("missing pointer key should yield nil, false")
	}

	structs := New[record](1)
	structs.Put([]byte("r"), record{n: 2, s: "two"})
	if got, ok := structs.Get([]byte("r")); !ok || got != (record{n: 2, s: "two"}) {
		t.Fatal("struct value round trip failed")
	}

	slicesMap := New[[]byte](1)
	slicesMap.Put([]byte("r"), []byte("payload"))
	if got, ok := slicesMap.Delete([]byte("r")); !ok || string(got) != "payload" {
		t.Fatal("slice value round trip failed")
	}
}
