package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/client"
)

func seededAPI() *fakeAPI {
	api := newFakeAPI()
	ctx := context.Background()
	_, _ = api.Put(ctx, []byte("app/a"), []byte("1"))
	_, _ = api.Put(ctx, []byte("app/b"), []byte("two words"))
	_, _ = api.Put(ctx, []byte("app/b"), []byte("2"))
	_, _ = api.Put(ctx, []byte("zed"), []byte{0x00, 0xff})
	api.calls = nil
	return api
}

func TestGetCommand(t *testing.T) {
	t.Parallel()
	t.Run("text", func(t *testing.T) {
		t.Parallel()
		api := seededAPI()
		res := testRun(t, api, "get", "app/b")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		want := "key              app/b\nvalue            2\nversion          2\ncreate_revision  2\nmod_revision     3\n"
		if res.stdout != want {
			t.Fatalf("stdout:\n%q\nwant:\n%q", res.stdout, want)
		}
		if api.lastNOpts != 0 {
			t.Fatalf("plain get should pass no options, got %d", api.lastNOpts)
		}
	})
	t.Run("stale flag after key", func(t *testing.T) {
		t.Parallel()
		api := seededAPI()
		res := testRun(t, api, "get", "app/a", "--stale")
		if res.code != exitOK || api.lastNOpts != 1 {
			t.Fatalf("code=%d opts=%d stderr=%q", res.code, api.lastNOpts, res.stderr)
		}
	})
	t.Run("json", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "--json", "get", "app/a")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		var got struct {
			KV kvJSON `json:"kv"`
		}
		if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
			t.Fatalf("invalid json %q: %v", res.stdout, err)
		}
		if got.KV.Key != "app/a" || got.KV.Value != "1" || got.KV.Version != 1 || got.KV.ModRevision != 1 {
			t.Fatalf("unexpected json %+v", got.KV)
		}
	})
	t.Run("binary value is quoted", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "get", "zed")
		if !strings.Contains(res.stdout, `value            "\x00\xff"`) {
			t.Fatalf("binary value should be quoted, got %q", res.stdout)
		}
	})
	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "get", "missing")
		if res.code != exitFailure || !strings.Contains(res.stderr, `key "missing" not found`) {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		if res.stdout != "" {
			t.Fatalf("nothing should be printed on stdout, got %q", res.stdout)
		}
	})
}

func TestPutCommand(t *testing.T) {
	t.Parallel()
	api := seededAPI()
	res := testRun(t, api, "put", "app/a", "new value", "--prev-kv")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	if string(api.lastKey) != "app/a" || string(api.lastValue) != "new value" || api.lastNOpts != 1 {
		t.Fatalf("Put called with key=%q value=%q opts=%d", api.lastKey, api.lastValue, api.lastNOpts)
	}
	want := "ok                    true\nrevision              5\nversion               2\nprev_key              app/a\nprev_value            1\nprev_version          1\nprev_create_revision  1\nprev_mod_revision     1\n"
	if res.stdout != want {
		t.Fatalf("stdout:\n%q\nwant:\n%q", res.stdout, want)
	}

	res = testRun(t, api, "--json", "put", "fresh", "v")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	var got struct {
		Header headerJSON `json:"header"`
		KV     kvJSON     `json:"kv"`
		PrevKV *kvJSON    `json:"prev_kv"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
		t.Fatalf("invalid json %q: %v", res.stdout, err)
	}
	if got.Header.Revision != 6 || got.KV.Key != "fresh" || got.KV.Version != 1 || got.PrevKV != nil {
		t.Fatalf("unexpected json %+v", got)
	}

	res = testRun(t, api, "put", "--", "--dashed", "v")
	if res.code != exitOK || string(api.lastKey) != "--dashed" {
		t.Fatalf("double dash should allow dashed positionals: code=%d key=%q stderr=%q", res.code, api.lastKey, res.stderr)
	}
}

func TestDelCommand(t *testing.T) {
	t.Parallel()
	api := seededAPI()
	res := testRun(t, api, "del", "app/a", "--prev-kv")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	if !strings.HasPrefix(res.stdout, "deleted               true\nrevision              5\nprev_key              app/a\n") {
		t.Fatalf("unexpected stdout %q", res.stdout)
	}
	res = testRun(t, api, "--json", "del", "app/a")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	var got struct {
		Deleted bool `json:"deleted"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil || got.Deleted {
		t.Fatalf("second delete should report deleted=false: %q (%v)", res.stdout, err)
	}
}

func TestCASCommand(t *testing.T) {
	t.Parallel()
	api := seededAPI()
	res := testRun(t, api, "cas", "app/a", "1", "swapped")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	if api.casVersion != 1 || string(api.lastValue) != "swapped" {
		t.Fatalf("CAS called with version=%d value=%q", api.casVersion, api.lastValue)
	}
	if !strings.HasPrefix(res.stdout, "succeeded  true\n") {
		t.Fatalf("unexpected stdout %q", res.stdout)
	}

	res = testRun(t, api, "cas", "app/a", "1", "again")
	if res.code != exitFailure {
		t.Fatalf("failed CAS should exit 1, got %d", res.code)
	}
	if !strings.HasPrefix(res.stdout, "succeeded  false\n") || !strings.Contains(res.stdout, "value      swapped") {
		t.Fatalf("failed CAS should print the current value, got %q", res.stdout)
	}
	if res.stderr != "" {
		t.Fatalf("failed CAS is not an error, stderr %q", res.stderr)
	}

	res = testRun(t, api, "--json", "cas", "nokey", "0", "created")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	var got struct {
		Succeeded bool `json:"succeeded"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil || !got.Succeeded {
		t.Fatalf("create-if-absent should succeed: %q (%v)", res.stdout, err)
	}
}

func TestParseScanRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                        string
		prefix, start, end          string
		prefixSet, startSet, endSet bool
		wantStart, wantEnd          string
		wantPrefix, wantErr         bool
	}{
		{name: "none", wantStart: "", wantEnd: ""},
		{name: "prefix", prefix: "app/", prefixSet: true, wantStart: "app/", wantPrefix: true},
		{name: "empty prefix", prefix: "", prefixSet: true, wantPrefix: true},
		{name: "start only", start: "m", startSet: true, wantStart: "m"},
		{name: "end only", end: "m", endSet: true, wantEnd: "m"},
		{name: "start and end", start: "a", end: "b", startSet: true, endSet: true, wantStart: "a", wantEnd: "b"},
		{name: "prefix and start", prefix: "p", start: "a", prefixSet: true, startSet: true, wantErr: true},
		{name: "prefix and end", prefix: "p", end: "z", prefixSet: true, endSet: true, wantErr: true},
		{name: "inverted range", start: "b", end: "a", startSet: true, endSet: true, wantErr: true},
		{name: "empty range", start: "a", end: "a", startSet: true, endSet: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, err := parseScanRange(tc.prefix, tc.start, tc.end, tc.prefixSet, tc.startSet, tc.endSet)
			if tc.wantErr {
				var ee *exitError
				if !errors.As(err, &ee) || ee.code != exitUsage {
					t.Fatalf("err = %v, want usage error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if string(r.start) != tc.wantStart || string(r.end) != tc.wantEnd || r.prefix != tc.wantPrefix {
				t.Fatalf("range = %+v, want start=%q end=%q prefix=%v", r, tc.wantStart, tc.wantEnd, tc.wantPrefix)
			}
		})
	}
}

func TestScanCommand(t *testing.T) {
	t.Parallel()
	t.Run("prefix", func(t *testing.T) {
		t.Parallel()
		api := seededAPI()
		res := testRun(t, api, "scan", "--prefix", "app/")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		if string(api.lastStart) != "app/" || api.lastEnd != nil || !api.lastPrefix {
			t.Fatalf("prefix scan should pass start=prefix, nil end and WithPrefix; got start=%q end=%q prefix=%v", api.lastStart, api.lastEnd, api.lastPrefix)
		}
		want := "KEY    VALUE  VERSION  CREATE_REVISION  MOD_REVISION\napp/a  1      1        1                1\napp/b  2      2        2                3\n"
		if res.stdout != want {
			t.Fatalf("stdout:\n%q\nwant:\n%q", res.stdout, want)
		}
	})
	t.Run("range and limit", func(t *testing.T) {
		t.Parallel()
		api := seededAPI()
		res := testRun(t, api, "scan", "--start", "app/b", "--end", "zz", "--limit", "5", "--stale")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		if string(api.lastStart) != "app/b" || string(api.lastEnd) != "zz" || api.lastPrefix || api.lastNOpts != 2 {
			t.Fatalf("range scan args: start=%q end=%q prefix=%v opts=%d", api.lastStart, api.lastEnd, api.lastPrefix, api.lastNOpts)
		}
		if !strings.Contains(res.stdout, "app/b") || !strings.Contains(res.stdout, `"\x00\xff"`) || strings.Contains(res.stdout, "app/a") {
			t.Fatalf("unexpected stdout %q", res.stdout)
		}
	})
	t.Run("prefix with start is a usage error", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "scan", "--prefix", "a", "--start", "b")
		if res.code != exitUsage || !strings.Contains(res.stderr, "--prefix cannot be combined") {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
	t.Run("negative limit", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "scan", "--limit", "-1")
		if res.code != exitUsage {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
	t.Run("positional is rejected", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, seededAPI(), "scan", "app/")
		if res.code != exitUsage || !strings.Contains(res.stderr, `unexpected argument "app/"`) {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
	t.Run("json and more", func(t *testing.T) {
		t.Parallel()
		api := seededAPI()
		res := testRun(t, api, "--json", "scan")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		var got struct {
			KVs  []kvJSON `json:"kvs"`
			More bool     `json:"more"`
		}
		if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
			t.Fatalf("invalid json %q: %v", res.stdout, err)
		}
		if len(got.KVs) != 3 || got.More {
			t.Fatalf("unexpected json %+v", got)
		}
	})
}

func TestScanMoreLine(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	api := seededAPI()
	// Simulate a truncated result by reporting More through a wrapper.
	more := &moreAPI{fakeAPI: api}
	if err := cmdScan(context.Background(), more, globals{}, []string{"--limit", "1"}, &out); err != nil {
		t.Fatalf("cmdScan: %v", err)
	}
	if !strings.HasSuffix(out.String(), "(more keys available; raise --limit or narrow the range)\n") {
		t.Fatalf("truncated scans should say so, got %q", out.String())
	}
}

// moreAPI forces More=true on scan results.
type moreAPI struct{ *fakeAPI }

func (m *moreAPI) Scan(ctx context.Context, start, end []byte, opts ...client.ReadOption) (*client.ScanResult, error) {
	res, err := m.fakeAPI.Scan(ctx, start, end, opts...)
	if err != nil {
		return nil, err
	}
	res.More = true
	return res, nil
}

func TestWatchCommand(t *testing.T) {
	t.Parallel()
	events := []client.WatchEvent{
		{Type: client.EventPut, KV: client.KeyValue{Key: []byte("k"), Value: []byte("v"), Version: 1, ModRevision: 7}, Header: client.Header{MemberID: 1, Revision: 7}},
		{Type: client.EventDelete, KV: client.KeyValue{Key: []byte("k"), ModRevision: 8}, PrevKV: &client.KeyValue{Key: []byte("k"), Value: []byte("v")}},
	}
	t.Run("text", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.watchEvents = events
		res := testRun(t, api, "watch", "k", "--prefix")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		want := "PUT    key=k value=v version=1 mod_revision=7\nDELETE key=k mod_revision=8\n"
		if res.stdout != want {
			t.Fatalf("stdout:\n%q\nwant:\n%q", res.stdout, want)
		}
		if api.lastNOpts != 1 || string(api.lastKey) != "k" {
			t.Fatalf("watch should pass WithPrefix: key=%q opts=%d", api.lastKey, api.lastNOpts)
		}
	})
	t.Run("json", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.watchEvents = events
		res := testRun(t, api, "--json", "watch", "k")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		dec := json.NewDecoder(strings.NewReader(res.stdout))
		var types []string
		for dec.More() {
			var ev struct {
				Type   string  `json:"type"`
				KV     kvJSON  `json:"kv"`
				PrevKV *kvJSON `json:"prev_kv"`
			}
			if err := dec.Decode(&ev); err != nil {
				t.Fatalf("invalid json stream %q: %v", res.stdout, err)
			}
			types = append(types, ev.Type)
			if ev.Type == "DELETE" && (ev.PrevKV == nil || ev.PrevKV.Value != "v") {
				t.Fatalf("delete event should carry prev_kv: %+v", ev)
			}
		}
		if len(types) != 2 || types[0] != "PUT" || types[1] != "DELETE" {
			t.Fatalf("event types = %v", types)
		}
	})
	t.Run("terminal error", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.watchEvents = []client.WatchEvent{events[0], {Err: errBoom}}
		res := testRun(t, api, "watch", "k")
		if res.code != exitFailure || !strings.Contains(res.stderr, "boom") {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		if !strings.HasPrefix(res.stdout, "PUT") {
			t.Fatalf("events before the error should be printed, got %q", res.stdout)
		}
	})
	t.Run("interrupt is a clean exit", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.watchBlocks = true
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		res := testRunCtx(t, ctx, api, nil, "watch", "k")
		if res.code != exitOK || res.stderr != "" {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
	t.Run("watch open error", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.err = errBoom
		res := testRun(t, api, "watch", "k")
		if res.code != exitFailure {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
}
