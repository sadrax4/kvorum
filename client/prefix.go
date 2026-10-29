package client

// PrefixEnd returns the smallest key that is greater than every key starting
// with prefix, so that the half-open range [prefix, PrefixEnd(prefix)) covers
// exactly the keys with that prefix. Trailing 0xff bytes are dropped before
// the last remaining byte is incremented. It returns nil, meaning "unbounded",
// for an empty prefix and for a prefix made only of 0xff bytes.
func PrefixEnd(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xff {
			continue
		}
		end := make([]byte, i+1)
		copy(end, prefix[:i+1])
		end[i]++
		return end
	}
	return nil
}
