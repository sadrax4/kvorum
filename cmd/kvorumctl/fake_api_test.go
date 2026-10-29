package main

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/sadrax4/kvorum/client"
)

// fakeAPI is an in-memory kvorumAPI that records how it was called.
type fakeAPI struct {
	mu sync.Mutex

	kvs     map[string]client.KeyValue
	rev     uint64
	members []client.Member
	leader  uint64
	status  *client.Status

	err error // returned by every call when set

	calls      []string
	lastKey    []byte
	lastValue  []byte
	lastStart  []byte
	lastEnd    []byte
	lastPrefix bool
	lastNOpts  int
	lastMember client.Member
	lastID     uint64
	casVersion uint64

	watchEvents []client.WatchEvent
	watchBlocks bool
	closed      bool
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{kvs: make(map[string]client.KeyValue), members: []client.Member{
		{ID: 1, RaftAddress: "10.0.0.1:2380", ClientAddress: "10.0.0.1:2379"},
		{ID: 2, RaftAddress: "10.0.0.2:2380", ClientAddress: "10.0.0.2:2379", Learner: true},
	}, leader: 1}
}

func (f *fakeAPI) record(name string) {
	f.calls = append(f.calls, name)
}

func (f *fakeAPI) header() client.Header {
	return client.Header{MemberID: 1, Term: 3, Revision: f.rev}
}

func (f *fakeAPI) Get(_ context.Context, key []byte, opts ...client.ReadOption) (*client.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Get")
	f.lastKey, f.lastNOpts = key, len(opts)
	if f.err != nil {
		return nil, f.err
	}
	kv, ok := f.kvs[string(key)]
	if !ok {
		return nil, client.ErrNotFound
	}
	return &kv, nil
}

func (f *fakeAPI) Put(_ context.Context, key, value []byte, opts ...client.WriteOption) (*client.PutResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Put")
	f.lastKey, f.lastValue, f.lastNOpts = key, value, len(opts)
	if f.err != nil {
		return nil, f.err
	}
	f.rev++
	prev, had := f.kvs[string(key)]
	kv := client.KeyValue{Key: bytes.Clone(key), Value: bytes.Clone(value), Version: 1, CreateRevision: f.rev, ModRevision: f.rev}
	if had {
		kv.Version = prev.Version + 1
		kv.CreateRevision = prev.CreateRevision
	}
	f.kvs[string(key)] = kv
	res := &client.PutResult{Header: f.header(), KV: &kv}
	if had && len(opts) > 0 {
		res.PrevKV = &prev
	}
	return res, nil
}

func (f *fakeAPI) Delete(_ context.Context, key []byte, opts ...client.WriteOption) (*client.DeleteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Delete")
	f.lastKey, f.lastNOpts = key, len(opts)
	if f.err != nil {
		return nil, f.err
	}
	prev, had := f.kvs[string(key)]
	res := &client.DeleteResult{Header: f.header(), Deleted: had}
	if had {
		f.rev++
		res.Header = f.header()
		delete(f.kvs, string(key))
		if len(opts) > 0 {
			res.PrevKV = &prev
		}
	}
	return res, nil
}

func (f *fakeAPI) CompareAndSwap(_ context.Context, key []byte, expectedVersion uint64, value []byte) (*client.CASResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CompareAndSwap")
	f.lastKey, f.lastValue, f.casVersion = key, value, expectedVersion
	if f.err != nil {
		return nil, f.err
	}
	cur, had := f.kvs[string(key)]
	var curVersion uint64
	if had {
		curVersion = cur.Version
	}
	if curVersion != expectedVersion {
		res := &client.CASResult{Header: f.header()}
		if had {
			res.KV = &cur
		}
		return res, nil
	}
	f.rev++
	kv := client.KeyValue{Key: bytes.Clone(key), Value: bytes.Clone(value), Version: curVersion + 1, CreateRevision: f.rev, ModRevision: f.rev}
	if had {
		kv.CreateRevision = cur.CreateRevision
	}
	f.kvs[string(key)] = kv
	return &client.CASResult{Header: f.header(), Succeeded: true, KV: &kv}, nil
}

func (f *fakeAPI) Scan(_ context.Context, start, end []byte, opts ...client.ReadOption) (*client.ScanResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Scan")
	f.lastStart, f.lastEnd, f.lastNOpts = start, end, len(opts)
	f.lastPrefix = false
	for _, o := range opts {
		if _, ok := o.(client.PrefixOption); ok {
			f.lastPrefix = true
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.lastPrefix {
		end = client.PrefixEnd(start)
	}
	keys := make([]string, 0, len(f.kvs))
	for k := range f.kvs {
		if bytes.Compare([]byte(k), start) < 0 || (len(end) > 0 && bytes.Compare([]byte(k), end) >= 0) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	res := &client.ScanResult{Header: f.header()}
	for _, k := range keys {
		res.KVs = append(res.KVs, f.kvs[k])
	}
	return res, nil
}

func (f *fakeAPI) Watch(ctx context.Context, key []byte, opts ...client.WatchOption) (<-chan client.WatchEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Watch")
	f.lastKey, f.lastNOpts = key, len(opts)
	if f.err != nil {
		return nil, f.err
	}
	ch := make(chan client.WatchEvent, len(f.watchEvents)+1)
	for _, ev := range f.watchEvents {
		ch <- ev
	}
	if f.watchBlocks {
		go func() {
			<-ctx.Done()
			ch <- client.WatchEvent{Err: ctx.Err()}
			close(ch)
		}()
		return ch, nil
	}
	close(ch)
	return ch, nil
}

func (f *fakeAPI) Status(context.Context) (*client.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Status")
	if f.err != nil {
		return nil, f.err
	}
	if f.status != nil {
		return f.status, nil
	}
	return &client.Status{
		Header: f.header(), ID: 1, State: "leader", Term: 3, Leader: 1,
		CommitIndex: 10, AppliedIndex: 10, LastIndex: 11, SnapshotIndex: 5,
		Members: f.members, Version: "v1.2.3",
	}, nil
}

func (f *fakeAPI) Members(context.Context) ([]client.Member, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Members")
	if f.err != nil {
		return nil, 0, f.err
	}
	return f.members, f.leader, nil
}

func (f *fakeAPI) AddMember(_ context.Context, m client.Member) ([]client.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("AddMember")
	f.lastMember = m
	if f.err != nil {
		return nil, f.err
	}
	f.members = append(f.members, m)
	return f.members, nil
}

func (f *fakeAPI) RemoveMember(_ context.Context, id uint64) ([]client.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("RemoveMember")
	f.lastID = id
	if f.err != nil {
		return nil, f.err
	}
	kept := f.members[:0:0]
	for _, m := range f.members {
		if m.ID != id {
			kept = append(kept, m)
		}
	}
	f.members = kept
	return f.members, nil
}

func (f *fakeAPI) PromoteMember(_ context.Context, id uint64) ([]client.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PromoteMember")
	f.lastID = id
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.members {
		if f.members[i].ID == id {
			f.members[i].Learner = false
		}
	}
	return f.members, nil
}

func (f *fakeAPI) TransferLeadership(_ context.Context, targetID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("TransferLeadership")
	f.lastID = targetID
	return f.err
}

func (f *fakeAPI) Snapshot(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Snapshot")
	if f.err != nil {
		return 0, f.err
	}
	return 42, nil
}

func (f *fakeAPI) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

var errBoom = errors.New("boom")
