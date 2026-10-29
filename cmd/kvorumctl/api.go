package main

import (
	"context"

	"github.com/sadrax4/kvorum/client"
)

// kvorumAPI is the subset of *client.Client the commands use. Tests
// substitute a fake implementation.
type kvorumAPI interface {
	Get(ctx context.Context, key []byte, opts ...client.ReadOption) (*client.KeyValue, error)
	Put(ctx context.Context, key, value []byte, opts ...client.WriteOption) (*client.PutResult, error)
	Delete(ctx context.Context, key []byte, opts ...client.WriteOption) (*client.DeleteResult, error)
	CompareAndSwap(ctx context.Context, key []byte, expectedVersion uint64, value []byte) (*client.CASResult, error)
	Scan(ctx context.Context, start, end []byte, opts ...client.ReadOption) (*client.ScanResult, error)
	Watch(ctx context.Context, key []byte, opts ...client.WatchOption) (<-chan client.WatchEvent, error)
	Status(ctx context.Context) (*client.Status, error)
	Members(ctx context.Context) ([]client.Member, uint64, error)
	AddMember(ctx context.Context, m client.Member) ([]client.Member, error)
	RemoveMember(ctx context.Context, id uint64) ([]client.Member, error)
	PromoteMember(ctx context.Context, id uint64) ([]client.Member, error)
	TransferLeadership(ctx context.Context, targetID uint64) error
	Snapshot(ctx context.Context) (uint64, error)
	Close() error
}

var _ kvorumAPI = (*client.Client)(nil)
