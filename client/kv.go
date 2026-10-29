package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/sadrax4/kvorum/api/kvorumpb"
)

func consistency(stale bool) kvorumpb.ReadConsistency {
	if stale {
		return kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE
	}
	return kvorumpb.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE
}

// Get returns the value stored at key. It returns ErrNotFound when the key
// does not exist. Reads are linearizable unless WithStale is given.
func (c *Client) Get(ctx context.Context, key []byte, opts ...ReadOption) (*KeyValue, error) {
	o := applyReadOptions(opts)
	req := &kvorumpb.GetRequest{Key: key, Consistency: consistency(o.stale)}
	var resp *kvorumpb.GetResponse
	err := c.invoke(ctx, "get", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewKVServiceClient(cc).Get(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetFound() || resp.GetKv() == nil {
		return nil, ErrNotFound
	}
	return kvFromProto(resp.GetKv()), nil
}

// Put stores value at key, creating the key or overwriting it. WithPrevKV
// returns the previous value in the result.
func (c *Client) Put(ctx context.Context, key, value []byte, opts ...WriteOption) (*PutResult, error) {
	o := applyWriteOptions(opts)
	sess := c.acquireSession()
	defer c.releaseSession(sess)
	req := &kvorumpb.PutRequest{Key: key, Value: value, Session: sess.next(), PrevKv: o.prevKV}
	var resp *kvorumpb.PutResponse
	err := c.invoke(ctx, "put", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewKVServiceClient(cc).Put(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &PutResult{
		Header: headerFromProto(resp.GetHeader()),
		KV:     kvFromProto(resp.GetKv()),
		PrevKV: kvFromProto(resp.GetPrevKv()),
	}, nil
}

// Delete removes key. Deleted is false in the result when the key did not
// exist. WithPrevKV returns the removed value in the result.
func (c *Client) Delete(ctx context.Context, key []byte, opts ...WriteOption) (*DeleteResult, error) {
	o := applyWriteOptions(opts)
	sess := c.acquireSession()
	defer c.releaseSession(sess)
	req := &kvorumpb.DeleteRequest{Key: key, Session: sess.next(), PrevKv: o.prevKV}
	var resp *kvorumpb.DeleteResponse
	err := c.invoke(ctx, "delete", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewKVServiceClient(cc).Delete(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &DeleteResult{
		Header:  headerFromProto(resp.GetHeader()),
		Deleted: resp.GetDeleted(),
		PrevKV:  kvFromProto(resp.GetPrevKv()),
	}, nil
}

// CompareAndSwap writes value at key only if the key's current version equals
// expectedVersion. An expectedVersion of zero means the key must not exist,
// which makes the call a create-if-absent primitive. A version mismatch is
// not an error: Succeeded is false and KV holds the current value.
func (c *Client) CompareAndSwap(ctx context.Context, key []byte, expectedVersion uint64, value []byte) (*CASResult, error) {
	sess := c.acquireSession()
	defer c.releaseSession(sess)
	req := &kvorumpb.CompareAndSwapRequest{
		Key:             key,
		ExpectedVersion: expectedVersion,
		Value:           value,
		Session:         sess.next(),
	}
	var resp *kvorumpb.CompareAndSwapResponse
	err := c.invoke(ctx, "compare-and-swap", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewKVServiceClient(cc).CompareAndSwap(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &CASResult{
		Header:    headerFromProto(resp.GetHeader()),
		Succeeded: resp.GetSucceeded(),
		KV:        kvFromProto(resp.GetKv()),
	}, nil
}

// Scan returns the keys in the half-open range [start, end) in ascending byte
// order. An empty start begins at the smallest key and an empty end extends to
// the largest. WithPrefix replaces end with PrefixEnd(start); WithLimit caps
// the number of keys and More reports whether the cap was hit.
func (c *Client) Scan(ctx context.Context, start, end []byte, opts ...ReadOption) (*ScanResult, error) {
	o := applyReadOptions(opts)
	if o.prefix {
		end = PrefixEnd(start)
	}
	req := &kvorumpb.ScanRequest{
		Start:       start,
		End:         end,
		Limit:       int64(o.limit),
		Consistency: consistency(o.stale),
	}
	var resp *kvorumpb.ScanResponse
	err := c.invoke(ctx, "scan", func(ctx context.Context, cc *grpc.ClientConn) error {
		var err error
		resp, err = kvorumpb.NewKVServiceClient(cc).Scan(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &ScanResult{
		Header: headerFromProto(resp.GetHeader()),
		KVs:    kvsFromProto(resp.GetKvs()),
		More:   resp.GetMore(),
	}, nil
}
