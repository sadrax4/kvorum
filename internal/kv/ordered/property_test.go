package ordered

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"
)

// modelEntry is one key of the reference model.
type modelEntry struct {
	key   []byte
	value int
}

// sliceModel is the obviously-correct reference implementation: a slice kept
// sorted by key, searched with binary search.
type sliceModel struct {
	entries []modelEntry
}

func (m *sliceModel) search(key []byte) (int, bool) {
	return slices.BinarySearchFunc(m.entries, key, func(e modelEntry, k []byte) int {
		return bytes.Compare(e.key, k)
	})
}

func (m *sliceModel) get(key []byte) (int, bool) {
	i, ok := m.search(key)
	if !ok {
		return 0, false
	}
	return m.entries[i].value, true
}

func (m *sliceModel) put(key []byte, v int) (int, bool) {
	i, ok := m.search(key)
	if ok {
		prev := m.entries[i].value
		m.entries[i].value = v
		return prev, true
	}
	m.entries = slices.Insert(m.entries, i, modelEntry{key: bytes.Clone(key), value: v})
	return 0, false
}

func (m *sliceModel) delete(key []byte) (int, bool) {
	i, ok := m.search(key)
	if !ok {
		return 0, false
	}
	v := m.entries[i].value
	m.entries = slices.Delete(m.entries, i, i+1)
	return v, true
}

func (m *sliceModel) ascend(start, end []byte, fn func(key []byte, v int) bool) {
	i, _ := m.search(start)
	for ; i < len(m.entries); i++ {
		e := m.entries[i]
		if len(end) > 0 && bytes.Compare(e.key, end) >= 0 {
			return
		}
		if !fn(e.key, e.value) {
			return
		}
	}
}

// randomKey draws keys from a small alphabet with lengths 1 through 6, which
// yields about a thousand distinct keys sharing many prefixes so that both
// hits and near misses are frequent.
func randomKey(rng *rand.Rand) []byte {
	k := make([]byte, 1+rng.IntN(6))
	for i := range k {
		k[i] = 'a' + byte(rng.IntN(3))
	}
	return k
}

type pair struct {
	key   string
	value int
}

func collectPairs(ascend func(func(key []byte, v int) bool), stopAfter int) []pair {
	var out []pair
	ascend(func(key []byte, v int) bool {
		out = append(out, pair{key: string(key), value: v})
		return stopAfter == 0 || len(out) < stopAfter
	})
	return out
}

// assertSameContents checks that a full iteration of m yields exactly the
// model's entries, comparing in place so the check stays cheap enough to run
// after every operation.
func assertSameContents(t *testing.T, op int, m *Map[int], model *sliceModel) {
	t.Helper()
	i := 0
	m.AscendAll(func(key []byte, v int) bool {
		if i >= len(model.entries) {
			t.Fatalf("op %d: map yields %q beyond the model's %d entries", op, key, len(model.entries))
		}
		if e := model.entries[i]; !bytes.Equal(e.key, key) || e.value != v {
			t.Fatalf("op %d: entry %d is (%q, %d), model has (%q, %d)", op, i, key, v, e.key, e.value)
		}
		i++
		return true
	})
	if i != len(model.entries) {
		t.Fatalf("op %d: map yields %d entries, model has %d", op, i, len(model.entries))
	}
}

func TestMapMatchesSortedSliceModel(t *testing.T) {
	t.Parallel()
	const operations = 20000
	rng := rand.New(rand.NewPCG(2024, 10))
	m := New[int](31)
	model := &sliceModel{}

	for i := range operations {
		key := randomKey(rng)
		switch op := rng.IntN(10); {
		case op < 5:
			v := rng.Int()
			gotPrev, gotReplaced := m.Put(key, v)
			wantPrev, wantReplaced := model.put(key, v)
			if gotPrev != wantPrev || gotReplaced != wantReplaced {
				t.Fatalf("op %d: Put(%q) = (%d, %t), want (%d, %t)", i, key, gotPrev, gotReplaced, wantPrev, wantReplaced)
			}
		case op < 8:
			gotV, gotOK := m.Get(key)
			wantV, wantOK := model.get(key)
			if gotV != wantV || gotOK != wantOK {
				t.Fatalf("op %d: Get(%q) = (%d, %t), want (%d, %t)", i, key, gotV, gotOK, wantV, wantOK)
			}
		default:
			gotV, gotOK := m.Delete(key)
			wantV, wantOK := model.delete(key)
			if gotV != wantV || gotOK != wantOK {
				t.Fatalf("op %d: Delete(%q) = (%d, %t), want (%d, %t)", i, key, gotV, gotOK, wantV, wantOK)
			}
		}

		if m.Len() != len(model.entries) {
			t.Fatalf("op %d: Len() = %d, want %d", i, m.Len(), len(model.entries))
		}
		assertSameContents(t, i, m, model)

		if i%50 == 0 {
			var start, end []byte
			if rng.IntN(4) != 0 {
				start = randomKey(rng)
			}
			if rng.IntN(4) != 0 {
				end = randomKey(rng)
			}
			stopAfter := rng.IntN(16)
			got := collectPairs(func(fn func([]byte, int) bool) { m.Ascend(start, end, fn) }, stopAfter)
			want := collectPairs(func(fn func([]byte, int) bool) { model.ascend(start, end, fn) }, stopAfter)
			if !slices.Equal(got, want) {
				t.Fatalf("op %d: Ascend(%q, %q) stop after %d = %v, want %v", i, start, end, stopAfter, got, want)
			}
		}
		if i%1000 == 0 {
			checkInvariants(t, m)
		}
	}
	checkInvariants(t, m)
	if m.Len() == 0 {
		t.Fatal("the random workload left the map empty; it did not exercise anything")
	}
}
