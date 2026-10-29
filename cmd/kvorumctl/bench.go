package main

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sadrax4/kvorum/client"
)

// Benchmark operation modes.
const (
	benchOpPut   = "put"
	benchOpGet   = "get"
	benchOpMixed = "mixed"
)

// benchConfig parameterises a benchmark run.
type benchConfig struct {
	op        string
	clients   int
	total     int
	keySpace  int
	valueSize int
}

func (c benchConfig) validate() error {
	switch c.op {
	case benchOpPut, benchOpGet, benchOpMixed:
	default:
		return usageErrorf("--op must be put, get or mixed, got %q", c.op)
	}
	if c.clients < 1 {
		return usageErrorf("--clients must be at least 1")
	}
	if c.total < 1 {
		return usageErrorf("--total must be at least 1")
	}
	if c.keySpace < 1 {
		return usageErrorf("--key-space must be at least 1")
	}
	if c.valueSize < 0 {
		return usageErrorf("--value-size must not be negative")
	}
	return nil
}

// benchReport is the outcome of a benchmark run.
type benchReport struct {
	Op         string        `json:"op"`
	Clients    int           `json:"clients"`
	Total      int           `json:"total"`
	Succeeded  int           `json:"succeeded"`
	Failed     int           `json:"failed"`
	Elapsed    time.Duration `json:"elapsed_ns"`
	Throughput float64       `json:"throughput_ops_per_sec"`
	P50        time.Duration `json:"p50_ns"`
	P95        time.Duration `json:"p95_ns"`
	P99        time.Duration `json:"p99_ns"`
	Max        time.Duration `json:"max_ns"`
	FirstError string        `json:"first_error,omitempty"`
}

// benchAPI is the part of the client a benchmark exercises.
type benchAPI interface {
	Put(ctx context.Context, key, value []byte, opts ...client.WriteOption) (*client.PutResult, error)
	Get(ctx context.Context, key []byte, opts ...client.ReadOption) (*client.KeyValue, error)
}

// workerResult collects what one benchmark goroutine measured.
type workerResult struct {
	latencies []time.Duration
	failed    int
	firstErr  error
}

// runBench executes cfg.total operations spread over cfg.clients goroutines
// and returns the aggregated report. It stops early when ctx ends; the
// operations completed so far are still reported.
func runBench(ctx context.Context, api benchAPI, cfg benchConfig) (benchReport, error) {
	if err := cfg.validate(); err != nil {
		return benchReport{}, err
	}
	value := make([]byte, cfg.valueSize)
	if _, err := cryptorand.Read(value); err != nil {
		return benchReport{}, fmt.Errorf("generate value: %w", err)
	}
	keys := make([][]byte, cfg.keySpace)
	for i := range keys {
		keys[i] = []byte("bench-" + strconv.Itoa(i))
	}

	var next atomic.Int64
	results := make([]workerResult, cfg.clients)
	var wg sync.WaitGroup
	start := time.Now()
	for w := range cfg.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[w] = benchWorker(ctx, api, cfg, keys, value, &next)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	report := benchReport{Op: cfg.op, Clients: cfg.clients, Total: cfg.total, Elapsed: elapsed}
	var all []time.Duration
	for _, r := range results {
		all = append(all, r.latencies...)
		report.Failed += r.failed
		if report.FirstError == "" && r.firstErr != nil {
			report.FirstError = r.firstErr.Error()
		}
	}
	report.Succeeded = len(all)
	slices.Sort(all)
	report.P50 = percentile(all, 50)
	report.P95 = percentile(all, 95)
	report.P99 = percentile(all, 99)
	report.Max = percentile(all, 100)
	if elapsed > 0 {
		report.Throughput = float64(report.Succeeded) / elapsed.Seconds()
	}
	return report, nil
}

func benchWorker(ctx context.Context, api benchAPI, cfg benchConfig, keys [][]byte, value []byte, next *atomic.Int64) workerResult {
	res := workerResult{latencies: make([]time.Duration, 0, cfg.total/cfg.clients+1)}
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // key selection needs no cryptographic randomness
	for ctx.Err() == nil {
		i := next.Add(1) - 1
		if i >= int64(cfg.total) {
			return res
		}
		key := keys[rng.IntN(len(keys))]
		doPut := cfg.op == benchOpPut || (cfg.op == benchOpMixed && i%2 == 0)
		began := time.Now()
		var err error
		if doPut {
			_, err = api.Put(ctx, key, value)
		} else {
			_, err = api.Get(ctx, key)
			if errors.Is(err, client.ErrNotFound) {
				err = nil
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return res
			}
			res.failed++
			if res.firstErr == nil {
				res.firstErr = err
			}
			continue
		}
		res.latencies = append(res.latencies, time.Since(began))
	}
	return res
}

// percentile returns the p-th percentile (0 < p <= 100) of sorted samples
// using the nearest-rank method. It returns zero for no samples.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

func cmdBench(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("bench")
	cfg := benchConfig{}
	fs.StringVar(&cfg.op, "op", benchOpPut, "operation")
	fs.IntVar(&cfg.clients, "clients", 8, "concurrent clients")
	fs.IntVar(&cfg.total, "total", 10000, "total operations")
	fs.IntVar(&cfg.keySpace, "key-space", 1000, "distinct keys")
	fs.IntVar(&cfg.valueSize, "value-size", 128, "value size in bytes")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	report, err := runBench(ctx, api, cfg)
	if err != nil {
		return err
	}
	if err := printBenchReport(out, g.json, report); err != nil {
		return err
	}
	if report.Succeeded == 0 {
		return errors.New("every operation failed: " + report.FirstError)
	}
	return nil
}

func printBenchReport(out io.Writer, asJSON bool, r benchReport) error {
	if asJSON {
		return printJSON(out, r)
	}
	fields := [][2]string{
		{"op", r.Op},
		{"clients", strconv.Itoa(r.Clients)},
		{"total", strconv.Itoa(r.Total)},
		{"succeeded", strconv.Itoa(r.Succeeded)},
		{"failed", strconv.Itoa(r.Failed)},
		{"elapsed", r.Elapsed.Round(time.Millisecond).String()},
		{"throughput", fmt.Sprintf("%.1f ops/s", r.Throughput)},
		{"p50", r.P50.Round(time.Microsecond).String()},
		{"p95", r.P95.Round(time.Microsecond).String()},
		{"p99", r.P99.Round(time.Microsecond).String()},
		{"max", r.Max.Round(time.Microsecond).String()},
	}
	if r.FirstError != "" {
		fields = append(fields, [2]string{"first_error", r.FirstError})
	}
	return printFields(out, fields)
}
