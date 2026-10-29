// Package ordered provides an in-memory ordered map keyed by byte slices.
//
// Map is a skip list (Pugh, "Skip Lists: A Probabilistic Alternative to
// Balanced Trees", 1990) with at most 32 levels and a promotion probability
// of 1/4. Lookups, insertions, and deletions take O(log n) expected time and
// iterating a range of k keys costs O(log n + k). Keys are ordered by
// [bytes.Compare].
//
// Level selection draws from a PCG source seeded through [New], so two maps
// created with the same seed and subjected to the same sequence of operations
// have identical internal structure. This makes tests and benchmarks
// reproducible; it has no effect on the logical contents of the map.
//
// A Map is not safe for concurrent use. It stores the key slices it is given
// without copying them and hands the same slices to iteration callbacks, so a
// key must not be modified while it is in the map.
package ordered

import (
	"bytes"
	"math/rand/v2"
)

const (
	// maxLevel is the tallest tower a node can have. With p = 1/4, 32 levels
	// keep searches logarithmic well beyond 2^32 keys.
	maxLevel = 32
	// levelMask selects the two random bits examined per level. A node climbs
	// one level while both bits are zero, which happens with probability 1/4.
	levelMask = 0b11
	// seedMix derives the second PCG state word from the seed so that every
	// seed selects a distinct, fully determined stream.
	seedMix = 0x9e3779b97f4a7c15
)

// node is one element of the list. next holds the forward pointer at every
// level the node participates in; len(next) is the node's height.
type node[V any] struct {
	key   []byte
	value V
	next  []*node[V]
}

// Map is an ordered map from byte-slice keys to values of type V. The zero
// value is not usable; create maps with [New].
type Map[V any] struct {
	head   *node[V]
	rng    *rand.Rand
	level  int
	length int
}

// New returns an empty map whose level choices are driven by a PCG source
// seeded with seed.
func New[V any](seed uint64) *Map[V] {
	return &Map[V]{
		head:  &node[V]{next: make([]*node[V], maxLevel)},
		rng:   rand.New(rand.NewPCG(seed, seed^seedMix)), //nolint:gosec // tower heights need reproducibility, not unpredictability
		level: 1,
	}
}

// Len returns the number of keys in the map.
func (m *Map[V]) Len() int {
	return m.length
}

// Get returns the value stored under key and whether the key is present.
func (m *Map[V]) Get(key []byte) (V, bool) {
	if n := m.search(key, nil); n != nil && bytes.Equal(n.key, key) {
		return n.value, true
	}
	var zero V
	return zero, false
}

// Put stores v under key. If the key was already present its value is
// replaced and the previous value is returned with replaced set to true; the
// key slice held by the map is kept, not the one passed to Put. Otherwise key
// itself is retained by the map and must not be modified afterwards.
func (m *Map[V]) Put(key []byte, v V) (prev V, replaced bool) {
	var path [maxLevel]*node[V]
	if n := m.search(key, &path); n != nil && bytes.Equal(n.key, key) {
		prev, n.value = n.value, v
		return prev, true
	}
	height := m.randomLevel()
	if height > m.level {
		for i := m.level; i < height; i++ {
			path[i] = m.head
		}
		m.level = height
	}
	n := &node[V]{key: key, value: v, next: make([]*node[V], height)}
	for i := range height {
		n.next[i] = path[i].next[i]
		path[i].next[i] = n
	}
	m.length++
	return prev, false
}

// Delete removes key and returns the value that was stored under it, or the
// zero value and false if the key was not present.
func (m *Map[V]) Delete(key []byte) (V, bool) {
	var path [maxLevel]*node[V]
	n := m.search(key, &path)
	if n == nil || !bytes.Equal(n.key, key) {
		var zero V
		return zero, false
	}
	// path[i] is the last node below key at level i; for every level n
	// participates in, its successor at that level is n itself.
	for i, next := range n.next {
		path[i].next[i] = next
	}
	for m.level > 1 && m.head.next[m.level-1] == nil {
		m.level--
	}
	m.length--
	return n.value, true
}

// search walks the list towards key. When path is non-nil it records, for
// every level, the last node whose key is smaller than key. The result is the
// first node at level 0 whose key is not smaller than key, or nil when every
// key is smaller.
//
// A node compared at a higher level is remembered in last so that it is not
// compared again when the descent reaches it at a lower level.
func (m *Map[V]) search(key []byte, path *[maxLevel]*node[V]) *node[V] {
	x := m.head
	var last *node[V]
	for i := m.level - 1; i >= 0; i-- {
		next := x.next[i]
		for next != nil && next != last && bytes.Compare(next.key, key) < 0 {
			x = next
			next = x.next[i]
		}
		last = next
		if path != nil {
			path[i] = x
		}
	}
	return last
}

// randomLevel draws a tower height in [1, maxLevel] with P(height > h) =
// 4^-h, consuming one 64-bit random value per call.
func (m *Map[V]) randomLevel() int {
	bits := m.rng.Uint64()
	height := 1
	for height < maxLevel && bits&levelMask == 0 {
		height++
		bits >>= 2
	}
	return height
}
