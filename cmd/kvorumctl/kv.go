package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/sadrax4/kvorum/client"
)

func cmdGet(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("get")
	stale := fs.Bool("stale", false, "stale read")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "key"); err != nil {
		return err
	}
	var opts []client.ReadOption
	if *stale {
		opts = append(opts, client.WithStale())
	}
	kv, err := api.Get(ctx, []byte(pos[0]), opts...)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return fmt.Errorf("key %q not found", pos[0])
		}
		return err
	}
	if g.json {
		return printJSON(out, struct {
			KV *kvJSON `json:"kv"`
		}{KV: kvView(kv)})
	}
	return printFields(out, kvFields("", kv))
}

func cmdPut(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("put")
	prevKV := fs.Bool("prev-kv", false, "return the previous value")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "key", "value"); err != nil {
		return err
	}
	var opts []client.WriteOption
	if *prevKV {
		opts = append(opts, client.WithPrevKV())
	}
	res, err := api.Put(ctx, []byte(pos[0]), []byte(pos[1]), opts...)
	if err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			Header headerJSON `json:"header"`
			KV     *kvJSON    `json:"kv"`
			PrevKV *kvJSON    `json:"prev_kv,omitempty"`
		}{Header: headerView(res.Header), KV: kvView(res.KV), PrevKV: kvView(res.PrevKV)})
	}
	fields := make([][2]string, 0, 8)
	fields = append(fields, [2]string{"ok", "true"}, [2]string{"revision", u64(res.Header.Revision)})
	if res.KV != nil {
		fields = append(fields, [2]string{"version", u64(res.KV.Version)})
	}
	fields = append(fields, kvFields("prev_", res.PrevKV)...)
	return printFields(out, fields)
}

func cmdDel(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("del")
	prevKV := fs.Bool("prev-kv", false, "return the deleted value")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "key"); err != nil {
		return err
	}
	var opts []client.WriteOption
	if *prevKV {
		opts = append(opts, client.WithPrevKV())
	}
	res, err := api.Delete(ctx, []byte(pos[0]), opts...)
	if err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			Header  headerJSON `json:"header"`
			Deleted bool       `json:"deleted"`
			PrevKV  *kvJSON    `json:"prev_kv,omitempty"`
		}{Header: headerView(res.Header), Deleted: res.Deleted, PrevKV: kvView(res.PrevKV)})
	}
	fields := make([][2]string, 0, 7)
	fields = append(fields, [2]string{"deleted", strconv.FormatBool(res.Deleted)}, [2]string{"revision", u64(res.Header.Revision)})
	fields = append(fields, kvFields("prev_", res.PrevKV)...)
	return printFields(out, fields)
}

func cmdCAS(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("cas")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "key", "expected-version", "value"); err != nil {
		return err
	}
	expected, err := strconv.ParseUint(pos[1], 10, 64)
	if err != nil {
		return usageErrorf("invalid expected version %q: must be an unsigned integer", pos[1])
	}
	res, err := api.CompareAndSwap(ctx, []byte(pos[0]), expected, []byte(pos[2]))
	if err != nil {
		return err
	}
	if g.json {
		if err := printJSON(out, struct {
			Header    headerJSON `json:"header"`
			Succeeded bool       `json:"succeeded"`
			KV        *kvJSON    `json:"kv,omitempty"`
		}{Header: headerView(res.Header), Succeeded: res.Succeeded, KV: kvView(res.KV)}); err != nil {
			return err
		}
	} else {
		fields := make([][2]string, 0, 4)
		fields = append(fields, [2]string{"succeeded", strconv.FormatBool(res.Succeeded)}, [2]string{"revision", u64(res.Header.Revision)})
		if res.KV != nil {
			fields = append(fields, [2]string{"version", u64(res.KV.Version)}, [2]string{"value", display(res.KV.Value)})
		}
		if err := printFields(out, fields); err != nil {
			return err
		}
	}
	if !res.Succeeded {
		return silentExit(exitFailure)
	}
	return nil
}

// scanRange holds the parsed range arguments of scan.
type scanRange struct {
	start, end []byte
	prefix     bool
}

// parseScanRange validates the mutually exclusive range flags of scan.
func parseScanRange(prefix, start, end string, prefixSet, startSet, endSet bool) (scanRange, error) {
	if prefixSet && (startSet || endSet) {
		return scanRange{}, usageErrorf("--prefix cannot be combined with --start or --end")
	}
	if prefixSet {
		return scanRange{start: []byte(prefix), prefix: true}, nil
	}
	r := scanRange{start: []byte(start), end: []byte(end)}
	if len(r.end) > 0 && string(r.start) >= string(r.end) {
		return scanRange{}, usageErrorf("--start must be smaller than --end")
	}
	return r, nil
}

func cmdScan(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("scan")
	prefix := fs.String("prefix", "", "key prefix")
	start := fs.String("start", "", "inclusive start")
	end := fs.String("end", "", "exclusive end")
	limit := fs.Int("limit", 0, "maximum keys")
	stale := fs.Bool("stale", false, "stale read")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	if *limit < 0 {
		return usageErrorf("--limit must not be negative")
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	r, err := parseScanRange(*prefix, *start, *end, set["prefix"], set["start"], set["end"])
	if err != nil {
		return err
	}
	opts := []client.ReadOption{client.WithLimit(*limit)}
	if r.prefix {
		opts = append(opts, client.WithPrefix())
	}
	if *stale {
		opts = append(opts, client.WithStale())
	}
	res, err := api.Scan(ctx, r.start, r.end, opts...)
	if err != nil {
		return err
	}
	if g.json {
		kvs := make([]kvJSON, 0, len(res.KVs))
		for i := range res.KVs {
			kvs = append(kvs, *kvView(&res.KVs[i]))
		}
		return printJSON(out, struct {
			Header headerJSON `json:"header"`
			KVs    []kvJSON   `json:"kvs"`
			More   bool       `json:"more"`
		}{Header: headerView(res.Header), KVs: kvs, More: res.More})
	}
	rows := make([][]string, 0, len(res.KVs))
	for _, kv := range res.KVs {
		rows = append(rows, []string{display(kv.Key), display(kv.Value), u64(kv.Version), u64(kv.CreateRevision), u64(kv.ModRevision)})
	}
	if err := printTable(out, []string{"KEY", "VALUE", "VERSION", "CREATE_REVISION", "MOD_REVISION"}, rows); err != nil {
		return err
	}
	if res.More {
		return writef(out, "(more keys available; raise --limit or narrow the range)\n")
	}
	return nil
}

func cmdWatch(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("watch")
	prefix := fs.Bool("prefix", false, "watch a prefix")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "key"); err != nil {
		return err
	}
	var opts []client.WatchOption
	if *prefix {
		opts = append(opts, client.WithPrefix())
	}
	events, err := api.Watch(ctx, []byte(pos[0]), opts...)
	if err != nil {
		return err
	}
	for ev := range events {
		if ev.Err != nil {
			// An interrupt ends the watch through ctx; that is a clean exit.
			if ctx.Err() == nil {
				return ev.Err
			}
			break
		}
		if err := printWatchEvent(out, g.json, ev); err != nil {
			return err
		}
	}
	return nil
}

func printWatchEvent(out io.Writer, asJSON bool, ev client.WatchEvent) error {
	if asJSON {
		return printJSON(out, struct {
			Type   string     `json:"type"`
			Header headerJSON `json:"header"`
			KV     *kvJSON    `json:"kv"`
			PrevKV *kvJSON    `json:"prev_kv,omitempty"`
		}{Type: ev.Type.String(), Header: headerView(ev.Header), KV: kvView(&ev.KV), PrevKV: kvView(ev.PrevKV)})
	}
	if ev.Type == client.EventDelete {
		return writef(out, "%-6s key=%s mod_revision=%d\n", ev.Type, display(ev.KV.Key), ev.KV.ModRevision)
	}
	return writef(out, "%-6s key=%s value=%s version=%d mod_revision=%d\n",
		ev.Type, display(ev.KV.Key), display(ev.KV.Value), ev.KV.Version, ev.KV.ModRevision)
}
