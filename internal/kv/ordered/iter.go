package ordered

import "bytes"

// Ascend calls fn for every entry whose key lies in the half-open range
// [start, end), in ascending key order, until fn returns false or the range is
// exhausted. An empty start begins at the smallest key and an empty end
// extends past the largest key, so Ascend(nil, nil, fn) visits everything.
// A start that is not smaller than a non-empty end selects nothing.
//
// The key passed to fn aliases the map's storage and must be neither modified
// nor retained beyond the call. fn must not modify the map.
func (m *Map[V]) Ascend(start, end []byte, fn func(key []byte, v V) bool) {
	for n := m.search(start, nil); n != nil; n = n.next[0] {
		if len(end) > 0 && bytes.Compare(n.key, end) >= 0 {
			return
		}
		if !fn(n.key, n.value) {
			return
		}
	}
}

// AscendAll calls fn for every entry in ascending key order until fn returns
// false. It is equivalent to Ascend(nil, nil, fn).
func (m *Map[V]) AscendAll(fn func(key []byte, v V) bool) {
	m.Ascend(nil, nil, fn)
}
