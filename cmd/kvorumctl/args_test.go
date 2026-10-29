package main

import (
	"errors"
	"slices"
	"testing"
)

func TestParseInterleaved(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []string
		wantPos  []string
		wantFlag bool
		wantN    int
		wantErr  error
		wantCode int
	}{
		{name: "no args", args: nil, wantPos: nil},
		{name: "positional only", args: []string{"a", "b"}, wantPos: []string{"a", "b"}},
		{name: "flag before", args: []string{"--flag", "a"}, wantPos: []string{"a"}, wantFlag: true},
		{name: "flag after", args: []string{"a", "--flag"}, wantPos: []string{"a"}, wantFlag: true},
		{name: "flag between", args: []string{"a", "-n", "3", "b"}, wantPos: []string{"a", "b"}, wantN: 3},
		{name: "flag with equals", args: []string{"a", "-n=4"}, wantPos: []string{"a"}, wantN: 4},
		{name: "double dash first", args: []string{"--", "--flag", "x"}, wantPos: []string{"--flag", "x"}},
		{name: "double dash later", args: []string{"a", "--flag", "--", "-n", "b"}, wantPos: []string{"a", "-n", "b"}, wantFlag: true},
		{name: "help", args: []string{"a", "-h"}, wantErr: errHelpRequested},
		{name: "unknown flag", args: []string{"a", "--nope"}, wantCode: exitUsage},
		{name: "bad int", args: []string{"-n", "x"}, wantCode: exitUsage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := newFlagSet("test")
			flag := fs.Bool("flag", false, "")
			n := fs.Int("n", 0, "")
			pos, err := parseInterleaved(fs, tc.args)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if tc.wantCode != 0 {
				var ee *exitError
				if !errors.As(err, &ee) || ee.code != tc.wantCode {
					t.Fatalf("err = %v, want exit code %d", err, tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if !slices.Equal(pos, tc.wantPos) {
				t.Fatalf("positional = %q, want %q", pos, tc.wantPos)
			}
			if *flag != tc.wantFlag || *n != tc.wantN {
				t.Fatalf("flag=%v n=%d, want flag=%v n=%d", *flag, *n, tc.wantFlag, tc.wantN)
			}
		})
	}
}

func TestWantArgs(t *testing.T) {
	t.Parallel()
	if err := wantArgs([]string{"a", "b"}, "x", "y"); err != nil {
		t.Fatalf("exact match: %v", err)
	}
	if err := wantArgs(nil); err != nil {
		t.Fatalf("no args expected and none given: %v", err)
	}
	err := wantArgs([]string{"a"}, "key", "value")
	var ee *exitError
	if !errors.As(err, &ee) || ee.code != exitUsage || ee.msg != "missing argument <value>" {
		t.Fatalf("missing arg error = %v", err)
	}
	err = wantArgs([]string{"a", "b"}, "key")
	if !errors.As(err, &ee) || ee.msg != `unexpected argument "b"` {
		t.Fatalf("extra arg error = %v", err)
	}
}

func TestParseID(t *testing.T) {
	t.Parallel()
	if id, err := parseID("12"); err != nil || id != 12 {
		t.Fatalf("parseID(12) = %d, %v", id, err)
	}
	for _, bad := range []string{"0", "-1", "abc", "", "1.5"} {
		if _, err := parseID(bad); err == nil {
			t.Fatalf("parseID(%q) should fail", bad)
		}
	}
}
