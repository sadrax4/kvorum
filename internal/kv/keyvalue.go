package kv

import "bytes"

// KeyValue is a key together with its value and version metadata.
type KeyValue struct {
	Key   []byte
	Value []byte
	// Version counts the writes to the key since it was created; the first
	// write stores 1. Deleting the key resets it.
	Version uint64
	// CreateRevision is the revision at which the key was created.
	CreateRevision uint64
	// ModRevision is the revision of the last write to the key.
	ModRevision uint64
}

// Clone returns a deep copy whose slices share no memory with kv.
func (kv KeyValue) Clone() KeyValue {
	kv.Key = bytes.Clone(kv.Key)
	kv.Value = cloneValue(kv.Value)
	return kv
}

// clonePtr deep-copies an optional KeyValue.
func clonePtr(kv *KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	c := kv.Clone()
	return &c
}

// Result is the outcome of applying a Command.
type Result struct {
	// KV is the key as stored after the command: set by OpPut, by a successful
	// OpCompareAndSwap, and by a failed OpCompareAndSwap when the key exists
	// (its current state). It is nil otherwise.
	KV *KeyValue
	// PrevKV is the key's state before the command. It is set only when the
	// command asked for it with Command.PrevKV and the key existed.
	PrevKV *KeyValue
	// Succeeded reports whether an OpCompareAndSwap matched its expected
	// version. It is false for other operations.
	Succeeded bool
	// Deleted reports whether an OpDelete removed an existing key.
	Deleted bool
	// Duplicate is set when the command was recognized as a retry and the
	// recorded result of the original execution was returned instead.
	Duplicate bool
	// Err is non-nil when the command was not executed: the revision was not
	// monotonic, the command was invalid, or its sequence number was stale.
	Err error
}

// clone deep-copies the result so that the copy can be handed out or stored
// without sharing slices with the original.
func (r Result) clone() Result {
	r.KV = clonePtr(r.KV)
	r.PrevKV = clonePtr(r.PrevKV)
	return r
}

// cloneValue copies a value, normalizing the empty value to nil so that
// identical logical values have identical representations.
func cloneValue(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}
