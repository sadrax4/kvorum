package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/client"
)

func TestPercentile(t *testing.T) {
	t.Parallel()
	ms := func(vs ...int) []time.Duration {
		out := make([]time.Duration, len(vs))
		for i, v := range vs {
			out[i] = time.Duration(v) * time.Millisecond
		}
		return out
	}
	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{name: "empty", sorted: nil, p: 50, want: 0},
		{name: "single p50", sorted: ms(7), p: 50, want: 7 * time.Millisecond},
		{name: "single p100", sorted: ms(7), p: 100, want: 7 * time.Millisecond},
		{name: "two p50", sorted: ms(1, 9), p: 50, want: 1 * time.Millisecond},
		{name: "two p99", sorted: ms(1, 9), p: 99, want: 9 * time.Millisecond},
		{name: "ten p50", sorted: ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p: 50, want: 5 * time.Millisecond},
		{name: "ten p95", sorted: ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p: 95, want: 10 * time.Millisecond},
		{name: "ten p99", sorted: ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p: 99, want: 10 * time.Millisecond},
		{name: "ten p100", sorted: ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p: 100, want: 10 * time.Millisecond},
		{name: "ten p10", sorted: ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), p: 10, want: 1 * time.Millisecond},
		{name: "hundred p99", sorted: seq(100), p: 99, want: 99 * time.Millisecond},
		{name: "hundred p95", sorted: seq(100), p: 95, want: 95 * time.Millisecond},
		{name: "zero p clamps to first", sorted: ms(3, 4), p: 0, want: 3 * time.Millisecond},
		{name: "over 100 clamps to last", sorted: ms(3, 4), p: 150, want: 4 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := percentile(tc.sorted, tc.p); got != tc.want {
				t.Fatalf("percentile(p=%v) = %v, want %v", tc.p, got, tc.want)
			}
		})
	}
}

// seq returns 1ms..n ms.
func seq(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i+1) * time.Millisecond
	}
	return out
}

// countingAPI counts puts and gets and can fail on demand.
type countingAPI struct {
	puts, gets atomic.Int64
	failEvery  int64
	mu         sync.Mutex
	keys       map[string]struct{}
}

func (c *countingAPI) Put(_ context.Context, key, _ []byte, _ ...client.WriteOption) (*client.PutResult, error) {
	n := c.puts.Add(1)
	c.mu.Lock()
	if c.keys == nil {
		c.keys = make(map[string]struct{})
	}
	c.keys[string(key)] = struct{}{}
	c.mu.Unlock()
	if c.failEvery > 0 && n%c.failEvery == 0 {
		return nil, errBoom
	}
	return &client.PutResult{}, nil
}

func (c *countingAPI) Get(context.Context, []byte, ...client.ReadOption) (*client.KeyValue, error) {
	c.gets.Add(1)
	return nil, client.ErrNotFound
}

func TestRunBench(t *testing.T) {
	t.Parallel()
	t.Run("put", func(t *testing.T) {
		t.Parallel()
		api := &countingAPI{}
		r, err := runBench(context.Background(), api, benchConfig{op: benchOpPut, clients: 4, total: 101, keySpace: 10, valueSize: 16})
		if err != nil {
			t.Fatalf("runBench: %v", err)
		}
		if api.puts.Load() != 101 || api.gets.Load() != 0 {
			t.Fatalf("puts=%d gets=%d, want 101/0", api.puts.Load(), api.gets.Load())
		}
		if r.Succeeded != 101 || r.Failed != 0 || r.Total != 101 || r.Clients != 4 || r.Op != benchOpPut {
			t.Fatalf("unexpected report %+v", r)
		}
		if r.Throughput <= 0 || r.Max < r.P99 || r.P99 < r.P95 || r.P95 < r.P50 {
			t.Fatalf("percentiles must be monotonic: %+v", r)
		}
		if len(api.keys) > 10 {
			t.Fatalf("key space should bound the keys, got %d", len(api.keys))
		}
	})
	t.Run("get treats not found as success", func(t *testing.T) {
		t.Parallel()
		api := &countingAPI{}
		r, err := runBench(context.Background(), api, benchConfig{op: benchOpGet, clients: 2, total: 50, keySpace: 5})
		if err != nil {
			t.Fatalf("runBench: %v", err)
		}
		if api.gets.Load() != 50 || r.Succeeded != 50 || r.Failed != 0 {
			t.Fatalf("gets=%d report=%+v", api.gets.Load(), r)
		}
	})
	t.Run("mixed splits evenly", func(t *testing.T) {
		t.Parallel()
		api := &countingAPI{}
		r, err := runBench(context.Background(), api, benchConfig{op: benchOpMixed, clients: 3, total: 100, keySpace: 5})
		if err != nil {
			t.Fatalf("runBench: %v", err)
		}
		if api.puts.Load() != 50 || api.gets.Load() != 50 || r.Succeeded != 100 {
			t.Fatalf("puts=%d gets=%d report=%+v", api.puts.Load(), api.gets.Load(), r)
		}
	})
	t.Run("failures are counted", func(t *testing.T) {
		t.Parallel()
		api := &countingAPI{failEvery: 5}
		r, err := runBench(context.Background(), api, benchConfig{op: benchOpPut, clients: 1, total: 20, keySpace: 1})
		if err != nil {
			t.Fatalf("runBench: %v", err)
		}
		if r.Failed != 4 || r.Succeeded != 16 || r.FirstError != "boom" {
			t.Fatalf("unexpected report %+v", r)
		}
	})
	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		bad := []benchConfig{
			{op: "nope", clients: 1, total: 1, keySpace: 1},
			{op: benchOpPut, clients: 0, total: 1, keySpace: 1},
			{op: benchOpPut, clients: 1, total: 0, keySpace: 1},
			{op: benchOpPut, clients: 1, total: 1, keySpace: 0},
			{op: benchOpPut, clients: 1, total: 1, keySpace: 1, valueSize: -1},
		}
		for _, cfg := range bad {
			_, err := runBench(context.Background(), &countingAPI{}, cfg)
			var ee *exitError
			if !errors.As(err, &ee) || ee.code != exitUsage {
				t.Fatalf("config %+v: err = %v, want usage error", cfg, err)
			}
		}
	})
	t.Run("cancelled context stops early", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		api := &countingAPI{}
		r, err := runBench(ctx, api, benchConfig{op: benchOpPut, clients: 2, total: 1000, keySpace: 1})
		if err != nil {
			t.Fatalf("runBench: %v", err)
		}
		if r.Succeeded > 2 || api.puts.Load() > 2 {
			t.Fatalf("a cancelled benchmark should stop at once, did %d puts", api.puts.Load())
		}
	})
}

func TestBenchReportFormatting(t *testing.T) {
	t.Parallel()
	r := benchReport{
		Op: "put", Clients: 8, Total: 1000, Succeeded: 998, Failed: 2,
		Elapsed: 1234 * time.Millisecond, Throughput: 808.75,
		P50: 900 * time.Microsecond, P95: 2 * time.Millisecond, P99: 5 * time.Millisecond, Max: 12 * time.Millisecond,
		FirstError: "boom",
	}
	var out strings.Builder
	if err := printBenchReport(&out, false, r); err != nil {
		t.Fatalf("printBenchReport: %v", err)
	}
	want := "op           put\nclients      8\ntotal        1000\nsucceeded    998\nfailed       2\nelapsed      1.234s\nthroughput   808.8 ops/s\np50          900µs\np95          2ms\np99          5ms\nmax          12ms\nfirst_error  boom\n"
	if out.String() != want {
		t.Fatalf("text report:\n%q\nwant:\n%q", out.String(), want)
	}

	out.Reset()
	if err := printBenchReport(&out, true, r); err != nil {
		t.Fatalf("printBenchReport(json): %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out.String()), &got); err != nil {
		t.Fatalf("invalid json %q: %v", out.String(), err)
	}
	if got["p99_ns"] != float64(5*time.Millisecond) || got["succeeded"] != float64(998) || got["first_error"] != "boom" {
		t.Fatalf("unexpected json %v", got)
	}
}

func TestBenchCommand(t *testing.T) {
	t.Parallel()
	api := newFakeAPI()
	res := testRun(t, api, "bench", "--op", "mixed", "--clients", "2", "--total", "40", "--key-space", "4", "--value-size", "8")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "op          mixed\n") || !strings.Contains(res.stdout, "succeeded   40\n") {
		t.Fatalf("unexpected report %q", res.stdout)
	}
	if n := len(api.calls); n != 40 {
		t.Fatalf("expected 40 operations, got %d", n)
	}

	api = newFakeAPI()
	api.err = errBoom
	res = testRun(t, api, "bench", "--total", "5", "--clients", "1")
	if res.code != exitFailure || !strings.Contains(res.stderr, "every operation failed: boom") {
		t.Fatalf("code=%d stderr=%q stdout=%q", res.code, res.stderr, res.stdout)
	}
	if !strings.Contains(res.stdout, "failed       5\n") {
		t.Fatalf("the report should still be printed, got %q", res.stdout)
	}

	res = testRun(t, newFakeAPI(), "bench", "extra")
	if res.code != exitUsage {
		t.Fatalf("positional args are a usage error, got %d", res.code)
	}
}
