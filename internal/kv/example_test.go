package kv_test

import (
	"fmt"

	"github.com/sadrax4/kvorum/internal/kv"
)

func ExampleStore() {
	store := kv.NewStore(kv.Options{})

	// Revisions are Raft log indexes: strictly increasing, assigned by the
	// caller.
	res := store.Apply(1, kv.Command{Op: kv.OpPut, Key: []byte("greeting"), Value: []byte("hello")})
	fmt.Println("created version", res.KV.Version, "at revision", res.KV.CreateRevision)

	res = store.Apply(2, kv.Command{Op: kv.OpPut, Key: []byte("greeting"), Value: []byte("hi"), PrevKV: true})
	fmt.Println("now version", res.KV.Version, "previously", string(res.PrevKV.Value))

	res = store.Apply(3, kv.Command{Op: kv.OpCompareAndSwap, Key: []byte("greeting"), Value: []byte("hey"), ExpectedVersion: 1})
	fmt.Println("stale compare-and-swap succeeded:", res.Succeeded)

	current, _ := store.Get([]byte("greeting"))
	fmt.Println(string(current.Value), current.Version, current.ModRevision)
	// Output:
	// created version 1 at revision 1
	// now version 2 previously hello
	// stale compare-and-swap succeeded: false
	// hi 2 2
}

func ExampleStore_Apply_exactlyOnce() {
	store := kv.NewStore(kv.Options{})
	cmd := kv.Command{Op: kv.OpPut, Key: []byte("counter"), Value: []byte("1"), ClientID: 42, Sequence: 1}

	first := store.Apply(1, cmd)
	retry := store.Apply(2, cmd) // the client did not see the first reply and retried
	fmt.Println(first.KV.Version, first.Duplicate)
	fmt.Println(retry.KV.Version, retry.Duplicate)
	// Output:
	// 1 false
	// 1 true
}

func ExampleStore_Watch() {
	store := kv.NewStore(kv.Options{})
	watcher := store.Watch([]byte("jobs/"), []byte("jobs0"), 16) // every key with the prefix "jobs/"
	defer watcher.Cancel()

	store.Apply(1, kv.Command{Op: kv.OpPut, Key: []byte("jobs/1"), Value: []byte("queued")})
	store.Apply(2, kv.Command{Op: kv.OpPut, Key: []byte("other"), Value: []byte("ignored")})
	store.Apply(3, kv.Command{Op: kv.OpDelete, Key: []byte("jobs/1")})

	for range 2 {
		ev := <-watcher.Events()
		fmt.Println(ev.Revision, ev.Type, string(ev.KV.Key))
	}
	// Output:
	// 1 put jobs/1
	// 3 delete jobs/1
}
