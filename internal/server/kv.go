package server

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
	"github.com/sadrax4/kvorum/internal/kv"
)

// watchBatch bounds how many events are coalesced into one WatchResponse.
const watchBatch = 64

type kvService struct {
	kvorumpb.UnimplementedKVServiceServer
	s *Server
}

func (k *kvService) header() *kvorumpb.ResponseHeader {
	return &kvorumpb.ResponseHeader{
		MemberId: uint64(k.s.node.ID()),
		Term:     uint64(k.s.node.Term()),
		Revision: uint64(k.s.node.AppliedIndex()),
	}
}

func toProtoKV(v *kv.KeyValue) *kvorumpb.KeyValue {
	if v == nil {
		return nil
	}
	return &kvorumpb.KeyValue{
		Key:            v.Key,
		Value:          v.Value,
		Version:        v.Version,
		CreateRevision: v.CreateRevision,
		ModRevision:    v.ModRevision,
	}
}

func session(sess *kvorumpb.Session) (clientID, sequence uint64) {
	if sess == nil {
		return 0, 0
	}
	return sess.GetClientId(), sess.GetSequence()
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(key) > kv.MaxKeySize {
		return status.Errorf(codes.InvalidArgument, "key exceeds %d bytes", kv.MaxKeySize)
	}
	return nil
}

// read performs the consistency barrier a read requires.
func (k *kvService) read(ctx context.Context, c kvorumpb.ReadConsistency) error {
	if c == kvorumpb.ReadConsistency_READ_CONSISTENCY_STALE {
		return nil
	}
	return k.s.node.LinearizableRead(ctx)
}

// Get implements kvorumpb.KVServiceServer.
func (k *kvService) Get(ctx context.Context, req *kvorumpb.GetRequest) (*kvorumpb.GetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	if err := k.read(ctx, req.GetConsistency()); err != nil {
		return nil, k.s.toStatus(err)
	}
	v, ok := k.s.node.Store().Get(req.GetKey())
	resp := &kvorumpb.GetResponse{Header: k.header(), Found: ok}
	if ok {
		resp.Kv = toProtoKV(&v)
	}
	return resp, nil
}

// Put implements kvorumpb.KVServiceServer.
func (k *kvService) Put(ctx context.Context, req *kvorumpb.PutRequest) (*kvorumpb.PutResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	clientID, seq := session(req.GetSession())
	res, err := k.s.node.Propose(ctx, kv.Command{
		Op:       kv.OpPut,
		Key:      req.GetKey(),
		Value:    req.GetValue(),
		ClientID: clientID,
		Sequence: seq,
		PrevKV:   req.GetPrevKv(),
	})
	if err != nil {
		return nil, k.s.toStatus(err)
	}
	return &kvorumpb.PutResponse{Header: k.header(), Kv: toProtoKV(res.KV), PrevKv: toProtoKV(res.PrevKV)}, nil
}

// Delete implements kvorumpb.KVServiceServer.
func (k *kvService) Delete(ctx context.Context, req *kvorumpb.DeleteRequest) (*kvorumpb.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	clientID, seq := session(req.GetSession())
	res, err := k.s.node.Propose(ctx, kv.Command{
		Op:       kv.OpDelete,
		Key:      req.GetKey(),
		ClientID: clientID,
		Sequence: seq,
		PrevKV:   req.GetPrevKv(),
	})
	if err != nil {
		return nil, k.s.toStatus(err)
	}
	return &kvorumpb.DeleteResponse{Header: k.header(), Deleted: res.Deleted, PrevKv: toProtoKV(res.PrevKV)}, nil
}

// CompareAndSwap implements kvorumpb.KVServiceServer.
func (k *kvService) CompareAndSwap(ctx context.Context, req *kvorumpb.CompareAndSwapRequest) (*kvorumpb.CompareAndSwapResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	clientID, seq := session(req.GetSession())
	res, err := k.s.node.Propose(ctx, kv.Command{
		Op:              kv.OpCompareAndSwap,
		Key:             req.GetKey(),
		Value:           req.GetValue(),
		ExpectedVersion: req.GetExpectedVersion(),
		ClientID:        clientID,
		Sequence:        seq,
	})
	if err != nil {
		return nil, k.s.toStatus(err)
	}
	return &kvorumpb.CompareAndSwapResponse{Header: k.header(), Succeeded: res.Succeeded, Kv: toProtoKV(res.KV)}, nil
}

// Scan implements kvorumpb.KVServiceServer.
func (k *kvService) Scan(ctx context.Context, req *kvorumpb.ScanRequest) (*kvorumpb.ScanResponse, error) {
	if len(req.GetStart()) > kv.MaxKeySize || len(req.GetEnd()) > kv.MaxKeySize {
		return nil, status.Errorf(codes.InvalidArgument, "range bounds exceed %d bytes", kv.MaxKeySize)
	}
	if req.GetLimit() < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must not be negative")
	}
	limit := int(req.GetLimit())
	if limit == 0 || limit > k.s.cfg.MaxScanLimit {
		limit = k.s.cfg.MaxScanLimit
	}
	if err := k.read(ctx, req.GetConsistency()); err != nil {
		return nil, k.s.toStatus(err)
	}
	kvs, more := k.s.node.Store().Scan(req.GetStart(), unboundedEnd(req.GetEnd()), limit)
	resp := &kvorumpb.ScanResponse{Header: k.header(), Kvs: make([]*kvorumpb.KeyValue, 0, len(kvs)), More: more}
	for i := range kvs {
		resp.Kvs = append(resp.Kvs, toProtoKV(&kvs[i]))
	}
	return resp, nil
}

// Watch implements kvorumpb.KVServiceServer.
func (k *kvService) Watch(req *kvorumpb.WatchRequest, stream grpc.ServerStreamingServer[kvorumpb.WatchResponse]) error {
	if err := validateKey(req.GetKey()); err != nil {
		return err
	}
	if len(req.GetRangeEnd()) > kv.MaxKeySize {
		return status.Errorf(codes.InvalidArgument, "range end exceeds %d bytes", kv.MaxKeySize)
	}
	w := k.s.node.Store().Watch(req.GetKey(), req.GetRangeEnd(), k.s.cfg.WatchBuffer)
	defer w.Cancel()
	k.s.metrics.KVWatchers.Inc()
	defer k.s.metrics.KVWatchers.Dec()

	ctx := stream.Context()
	for {
		var first kv.Event
		select {
		case <-ctx.Done():
			return nil
		case <-k.s.stopC:
			return status.Error(codes.Unavailable, "server shutting down")
		case ev, ok := <-w.Events():
			if !ok {
				return k.s.toStatus(fmt.Errorf("watch ended: %w", w.Err()))
			}
			first = ev
		}
		resp := &kvorumpb.WatchResponse{Header: k.header(), Events: []*kvorumpb.Event{toProtoEvent(first)}}
	drain:
		for len(resp.Events) < watchBatch {
			select {
			case ev, ok := <-w.Events():
				if !ok {
					break drain
				}
				resp.Events = append(resp.Events, toProtoEvent(ev))
			default:
				break drain
			}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// unboundedEnd maps the wire convention for "every key from start onwards"
// (a range end consisting of the single byte 0x00, which no real key can be
// smaller than) to the store's representation for Scan.
func unboundedEnd(end []byte) []byte {
	if len(end) == 1 && end[0] == 0 {
		return nil
	}
	return end
}

func toProtoEvent(ev kv.Event) *kvorumpb.Event {
	out := &kvorumpb.Event{Kv: toProtoKV(&ev.KV), PrevKv: toProtoKV(ev.PrevKV)}
	switch ev.Type {
	case kv.EventPut:
		out.Type = kvorumpb.Event_TYPE_PUT
	case kv.EventDelete:
		out.Type = kvorumpb.Event_TYPE_DELETE
	}
	return out
}

// requestDeadline bounds requests that arrive without a deadline so that a
// partitioned node does not hold them forever.
func requestDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
