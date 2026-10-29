package kv

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
)

func assertEvent(t *testing.T, ev Event, typ EventType, revision uint64, kv KeyValue, prev *KeyValue) {
	t.Helper()
	if ev.Type != typ || ev.Revision != revision {
		t.Fatalf("event = %s at revision %d, want %s at %d", ev.Type, ev.Revision, typ, revision)
	}
	if !kvEqual(ev.KV, kv) {
		t.Fatalf("event KV = %+v, want %+v", ev.KV, kv)
	}
	if prev == nil {
		assertNoKV(t, "event PrevKV", ev.PrevKV)
	} else {
		assertKV(t, "event PrevKV", ev.PrevKV, *prev)
	}
}

func TestWatchSingleKey(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("k"), nil, 16)
	if s.Watchers() != 1 {
		t.Fatalf("Watchers() = %d, want 1", s.Watchers())
	}

	mustApply(t, s, 1, put("other", "x"))
	assertNoEvent(t, w)

	mustApply(t, s, 2, put("k", "v1"))
	assertEvent(t, recvEvent(t, w), EventPut, 2, kv("k", "v1", 1, 2, 2), nil)

	mustApply(t, s, 3, put("k", "v2"))
	assertEvent(t, recvEvent(t, w), EventPut, 3, kv("k", "v2", 2, 2, 3), ptr(kv("k", "v1", 1, 2, 2)))

	mustApply(t, s, 4, put("kk", "x"))
	mustApply(t, s, 5, put("j", "x"))
	assertNoEvent(t, w)

	mustApply(t, s, 6, cas("k", "v3", 2))
	assertEvent(t, recvEvent(t, w), EventPut, 6, kv("k", "v3", 3, 2, 6), ptr(kv("k", "v2", 2, 2, 3)))

	mustApply(t, s, 7, del("k"))
	assertEvent(t, recvEvent(t, w), EventDelete, 7, KeyValue{Key: []byte("k"), ModRevision: 7}, ptr(kv("k", "v3", 3, 2, 6)))

	mustApply(t, s, 8, del("k"))
	assertNoEvent(t, w)
	if w.Err() != nil {
		t.Fatalf("Err() = %v while active", w.Err())
	}
	w.Cancel()
	assertClosed(t, w, ErrWatcherCancelled)
}

func TestWatchRange(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("b"), []byte("d"), 16)
	inverted := s.Watch([]byte("d"), []byte("b"), 16)
	empty := s.Watch([]byte("c"), []byte("c"), 16)

	for i, k := range []string{"a", "b", "bz", "c", "d", "e"} {
		mustApply(t, s, uint64(i+1), put(k, "v"))
	}
	for _, want := range []string{"b", "bz", "c"} {
		if ev := recvEvent(t, w); string(ev.KV.Key) != want {
			t.Fatalf("range watcher got %q, want %q", ev.KV.Key, want)
		}
	}
	assertNoEvent(t, w)
	assertNoEvent(t, inverted)
	assertNoEvent(t, empty)
	mustApply(t, s, 7, del("c"))
	if ev := recvEvent(t, w); ev.Type != EventDelete || string(ev.KV.Key) != "c" {
		t.Fatalf("range watcher got %+v, want delete of c", ev)
	}
	w.Cancel()
	inverted.Cancel()
	empty.Cancel()
	if s.Watchers() != 0 {
		t.Fatalf("Watchers() = %d after cancels, want 0", s.Watchers())
	}
}

func TestWatchUnboundedEnd(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	fromC := s.Watch([]byte("c"), []byte{0}, 16)
	all := s.Watch(nil, []byte{0}, 16)
	for i, k := range []string{"a", "b", "c", "d", "\xff\xff"} {
		mustApply(t, s, uint64(i+1), put(k, "v"))
	}
	for _, want := range []string{"c", "d", "\xff\xff"} {
		if ev := recvEvent(t, fromC); string(ev.KV.Key) != want {
			t.Fatalf("watcher from c got %q, want %q", ev.KV.Key, want)
		}
	}
	assertNoEvent(t, fromC)
	for _, want := range []string{"a", "b", "c", "d", "\xff\xff"} {
		if ev := recvEvent(t, all); string(ev.KV.Key) != want {
			t.Fatalf("watch-all got %q, want %q", ev.KV.Key, want)
		}
	}
	assertNoEvent(t, all)
}

func TestWatchEmptyKeyNeverFires(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch(nil, nil, 4)
	mustApply(t, s, 1, put("a", "v"))
	mustApply(t, s, 2, put("\x00", "v"))
	assertNoEvent(t, w)
	w.Cancel()
}

func TestWatchNoEventsWithoutMutation(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch(nil, []byte{0}, 16)
	mustApply(t, s, 1, withSession(put("k", "v"), 1, 1))
	recvEvent(t, w)

	mustApply(t, s, 2, withSession(put("k", "v"), 1, 1)) // duplicate
	if res := s.Apply(3, withSession(put("k", "v"), 1, 0)); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatal(res.Err)
	}
	mustApply(t, s, 4, cas("k", "x", 42)) // failed CAS
	mustApply(t, s, 5, del("missing"))
	if res := s.Apply(6, Command{Op: 9, Key: []byte("k")}); res.Err == nil {
		t.Fatal("invalid command accepted")
	}
	if res := s.Apply(6, put("k", "again")); !errors.Is(res.Err, ErrRevisionNotMonotonic) {
		t.Fatal(res.Err)
	}
	assertNoEvent(t, w)
}

func TestWatchOverflowCancelsWatcher(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("k"), nil, 1)
	mustApply(t, s, 1, put("k", "v1"))
	if w.Err() != nil {
		t.Fatalf("Err() = %v before overflow", w.Err())
	}
	mustApply(t, s, 2, put("k", "v2")) // does not block, cancels w
	if s.Watchers() != 0 {
		t.Fatalf("Watchers() = %d after overflow, want 0", s.Watchers())
	}
	assertEvent(t, recvEvent(t, w), EventPut, 1, kv("k", "v1", 1, 1, 1), nil)
	assertClosed(t, w, ErrWatcherOverflow)
	w.Cancel()
	if !errors.Is(w.Err(), ErrWatcherOverflow) {
		t.Fatalf("Err() = %v after Cancel, want the original overflow", w.Err())
	}
	mustApply(t, s, 3, put("k", "v3"))
	assertGet(t, s, kv("k", "v3", 3, 1, 3))
}

func TestWatchCancelIsIdempotent(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("a"), []byte("z"), 4)
	w.Cancel()
	w.Cancel()
	assertClosed(t, w, ErrWatcherCancelled)
	mustApply(t, s, 1, put("b", "v"))
	if s.Watchers() != 0 {
		t.Fatalf("Watchers() = %d, want 0", s.Watchers())
	}
	w.Cancel()
}

func TestWatchRemovalKeepsOtherWatchersIndexed(t *testing.T) {
	t.Parallel()
	for _, single := range []bool{true, false} {
		t.Run(fmt.Sprintf("single=%t", single), func(t *testing.T) {
			t.Parallel()
			s := NewStore(Options{})
			var end []byte
			if !single {
				end = []byte("z")
			}
			ws := make([]*Watcher, 5)
			for i := range ws {
				ws[i] = s.Watch([]byte("k"), end, 8)
			}
			ws[0].Cancel() // swaps the last watcher into slot 0
			ws[2].Cancel() // swaps the new last watcher into slot 2
			mustApply(t, s, 1, put("k", "v1"))
			for _, i := range []int{1, 3, 4} {
				if ev := recvEvent(t, ws[i]); ev.Revision != 1 {
					t.Fatalf("watcher %d got revision %d", i, ev.Revision)
				}
			}
			assertClosed(t, ws[0], ErrWatcherCancelled)
			assertClosed(t, ws[2], ErrWatcherCancelled)
			ws[4].Cancel()
			ws[1].Cancel()
			mustApply(t, s, 2, put("k", "v2"))
			if ev := recvEvent(t, ws[3]); ev.Revision != 2 {
				t.Fatalf("remaining watcher got revision %d", ev.Revision)
			}
			if s.Watchers() != 1 {
				t.Fatalf("Watchers() = %d, want 1", s.Watchers())
			}
			ws[3].Cancel()
			if s.Watchers() != 0 || len(s.keyWatchers) != 0 || len(s.rangeWatchers) != 0 {
				t.Fatalf("registries not empty: %d key lists, %d range watchers", len(s.keyWatchers), len(s.rangeWatchers))
			}
		})
	}
}

func TestWatchEventsArriveInRevisionOrder(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("a"), []byte("z"), 64)
	for rev := uint64(1); rev <= 50; rev++ {
		mustApply(t, s, rev, put(fmt.Sprintf("k%02d", rev%7), "v"))
	}
	for rev := uint64(1); rev <= 50; rev++ {
		if ev := recvEvent(t, w); ev.Revision != rev || ev.KV.ModRevision != rev {
			t.Fatalf("event revision %d (mod %d), want %d", ev.Revision, ev.KV.ModRevision, rev)
		}
	}
}

func TestWatchDefaultBuffer(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	for _, buffer := range []int{0, -1} {
		w := s.Watch([]byte("k"), nil, buffer)
		if cap(w.Events()) != DefaultWatchBuffer {
			t.Fatalf("buffer %d produced capacity %d, want %d", buffer, cap(w.Events()), DefaultWatchBuffer)
		}
		w.Cancel()
	}
	w := s.Watch([]byte("k"), nil, 3)
	if cap(w.Events()) != 3 {
		t.Fatalf("capacity = %d, want 3", cap(w.Events()))
	}
}

func TestWatchWatchersAreIndependent(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	slow := s.Watch([]byte("k"), nil, 1)
	fast := s.Watch([]byte("k"), nil, 10)
	for rev := uint64(1); rev <= 3; rev++ {
		mustApply(t, s, rev, put("k", "v"))
	}
	recvEvent(t, slow)
	assertClosed(t, slow, ErrWatcherOverflow)
	for rev := uint64(1); rev <= 3; rev++ {
		if ev := recvEvent(t, fast); ev.Revision != rev {
			t.Fatalf("fast watcher got revision %d, want %d", ev.Revision, rev)
		}
	}
	if fast.Err() != nil || s.Watchers() != 1 {
		t.Fatalf("fast watcher affected: err=%v watchers=%d", fast.Err(), s.Watchers())
	}
}

func TestWatchEventsDoNotAliasStorageOrResults(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	w := s.Watch([]byte("k"), nil, 8)
	mustApply(t, s, 1, put("k", "v1"))
	ev := recvEvent(t, w)
	ev.KV.Key[0], ev.KV.Value[0] = 'X', 'X'
	assertGet(t, s, kv("k", "v1", 1, 1, 1))

	res := mustApply(t, s, 2, withPrevKV(put("k", "v2")))
	ev = recvEvent(t, w)
	ev.PrevKV.Value[0], ev.KV.Value[0] = 'X', 'X'
	assertKV(t, "result PrevKV", res.PrevKV, kv("k", "v1", 1, 1, 1))
	assertKV(t, "result KV", res.KV, kv("k", "v2", 2, 1, 2))
	assertGet(t, s, kv("k", "v2", 2, 1, 2))

	res = mustApply(t, s, 3, withPrevKV(del("k")))
	ev = recvEvent(t, w)
	ev.PrevKV.Value[0] = 'X'
	assertKV(t, "delete PrevKV", res.PrevKV, kv("k", "v2", 2, 1, 2))
}

func TestWatchConcurrentCancelDuringApply(t *testing.T) {
	t.Parallel()
	s := NewStore(Options{})
	const writers, watchersPerGoroutine = 1, 50
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				for range watchersPerGoroutine {
					var end []byte
					if rng.IntN(2) == 0 {
						end = []byte{0}
					}
					w := s.Watch([]byte(fmt.Sprintf("k%d", rng.IntN(10))), end, 1+rng.IntN(3))
					if rng.IntN(2) == 0 {
						for range rng.IntN(4) {
							select {
							case <-w.Events():
							default:
							}
						}
					}
					w.Cancel()
					w.Cancel()
				}
			}
		}()
	}
	for rev := uint64(1); rev <= 3000*writers; rev++ {
		mustApply(t, s, rev, put(fmt.Sprintf("k%d", rev%10), "v"))
	}
	close(stop)
	wg.Wait()
	if s.Watchers() != 0 {
		t.Fatalf("Watchers() = %d after every watcher was cancelled", s.Watchers())
	}
}
