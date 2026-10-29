package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sadrax4/kvorum/client"
)

// runResult is the outcome of one invocation under test.
type runResult struct {
	code   int
	stdout string
	stderr string
}

// testRun executes run with a fake client and captured output.
func testRun(t *testing.T, api *fakeAPI, args ...string) runResult {
	t.Helper()
	return testRunCtx(t, context.Background(), api, nil, args...)
}

func testRunCtx(t *testing.T, ctx context.Context, api *fakeAPI, getenv func(string) string, args ...string) runResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	e := &env{
		stdout: &stdout,
		stderr: &stderr,
		getenv: getenv,
		dial: func(client.Config) (kvorumAPI, error) {
			return api, nil
		},
		ctx: ctx,
	}
	code := run(args, e)
	return runResult{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func TestRunHelpAndUsageErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "help", args: []string{"help"}, wantCode: exitOK, wantStdout: "Usage: kvorumctl"},
		{name: "-h", args: []string{"-h"}, wantCode: exitOK, wantStdout: "Usage: kvorumctl"},
		{name: "--help", args: []string{"--help"}, wantCode: exitOK, wantStdout: "Usage: kvorumctl"},
		{name: "help command", args: []string{"help", "scan"}, wantCode: exitOK, wantStdout: "--prefix P"},
		{name: "help unknown", args: []string{"help", "nope"}, wantCode: exitUsage, wantStderr: `unknown command "nope"`},
		{name: "command -h", args: []string{"get", "-h"}, wantCode: exitOK, wantStdout: "Usage: kvorumctl [global flags] get <key>"},
		{name: "no command", args: nil, wantCode: exitUsage, wantStderr: "Usage: kvorumctl"},
		{name: "unknown command", args: []string{"frobnicate"}, wantCode: exitUsage, wantStderr: `unknown command "frobnicate"`},
		{name: "unknown global flag", args: []string{"--bogus", "get", "k"}, wantCode: exitUsage, wantStderr: "flag provided but not defined: -bogus"},
		{name: "zero timeout", args: []string{"--timeout", "0", "status"}, wantCode: exitUsage, wantStderr: "--timeout must be positive"},
		{name: "empty endpoints", args: []string{"--endpoints", ",", "status"}, wantCode: exitUsage, wantStderr: "no endpoints given"},
		{name: "missing key", args: []string{"get"}, wantCode: exitUsage, wantStderr: "missing argument <key>"},
		{name: "extra arg", args: []string{"get", "a", "b"}, wantCode: exitUsage, wantStderr: `unexpected argument "b"`},
		{name: "unknown command flag", args: []string{"get", "a", "--nope"}, wantCode: exitUsage, wantStderr: "flag provided but not defined: -nope"},
		{name: "cas bad version", args: []string{"cas", "k", "x", "v"}, wantCode: exitUsage, wantStderr: "invalid expected version"},
		{name: "member no sub", args: []string{"member"}, wantCode: exitUsage, wantStderr: "missing member subcommand"},
		{name: "member bad sub", args: []string{"member", "frob"}, wantCode: exitUsage, wantStderr: `unknown member subcommand "frob"`},
		{name: "member add short", args: []string{"member", "add", "3"}, wantCode: exitUsage, wantStderr: "member add needs <id> and <raft-addr>"},
		{name: "member bad id", args: []string{"member", "remove", "zero"}, wantCode: exitUsage, wantStderr: `invalid id "zero"`},
		{name: "transfer zero id", args: []string{"transfer-leader", "0"}, wantCode: exitUsage, wantStderr: "must be greater than zero"},
		{name: "status extra arg", args: []string{"status", "x"}, wantCode: exitUsage, wantStderr: `unexpected argument "x"`},
		{name: "bench bad op", args: []string{"bench", "--op", "fly"}, wantCode: exitUsage, wantStderr: "--op must be put, get or mixed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := testRun(t, newFakeAPI(), tc.args...)
			if res.code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d (stdout %q, stderr %q)", res.code, tc.wantCode, res.stdout, res.stderr)
			}
			if tc.wantStdout != "" && !strings.Contains(res.stdout, tc.wantStdout) {
				t.Fatalf("stdout %q does not contain %q", res.stdout, tc.wantStdout)
			}
			if tc.wantStderr != "" && !strings.Contains(res.stderr, tc.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", res.stderr, tc.wantStderr)
			}
			if tc.wantCode == exitUsage && len(tc.args) > 0 && !strings.Contains(res.stderr, "Usage:") {
				t.Fatalf("usage errors should print usage on stderr, got %q", res.stderr)
			}
		})
	}
}

func TestRunGlobalFlags(t *testing.T) {
	t.Parallel()
	var got client.Config
	var stdout, stderr bytes.Buffer
	e := &env{
		stdout: &stdout,
		stderr: &stderr,
		getenv: func(k string) string {
			if k == endpointsEnvVar {
				return "env1:1, env2:2"
			}
			return ""
		},
		dial: func(cfg client.Config) (kvorumAPI, error) {
			got = cfg
			return newFakeAPI(), nil
		},
	}
	if code := run([]string{"status"}, e); code != exitOK {
		t.Fatalf("exit code = %d, stderr %q", code, stderr.String())
	}
	if len(got.Endpoints) != 2 || got.Endpoints[0] != "env1:1" || got.Endpoints[1] != "env2:2" {
		t.Fatalf("endpoints from env = %q", got.Endpoints)
	}
	if got.RequestTimeout != defaultTimeout || got.DialTimeout != defaultTimeout {
		t.Fatalf("timeouts = %v/%v, want %v", got.RequestTimeout, got.DialTimeout, defaultTimeout)
	}

	if code := run([]string{"--endpoints", "a:1,b:2,c:3", "--timeout", "250ms", "status"}, e); code != exitOK {
		t.Fatalf("exit code = %d, stderr %q", code, stderr.String())
	}
	if len(got.Endpoints) != 3 || got.Endpoints[2] != "c:3" {
		t.Fatalf("endpoints from flag = %q", got.Endpoints)
	}
	if got.RequestTimeout != 250*time.Millisecond {
		t.Fatalf("timeout = %v, want 250ms", got.RequestTimeout)
	}

	e.getenv = nil
	if code := run([]string{"status"}, e); code != exitOK {
		t.Fatalf("exit code = %d, stderr %q", code, stderr.String())
	}
	if len(got.Endpoints) != 1 || got.Endpoints[0] != defaultEndpoints {
		t.Fatalf("default endpoints = %q", got.Endpoints)
	}
}

func TestRunDialFailure(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	e := &env{stdout: &stdout, stderr: &stderr, dial: func(client.Config) (kvorumAPI, error) { return nil, errBoom }}
	if code := run([]string{"status"}, e); code != exitFailure {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "boom") {
		t.Fatalf("stderr %q should name the error", stderr.String())
	}
}

func TestRunClosesClientAndReportsErrors(t *testing.T) {
	t.Parallel()
	api := newFakeAPI()
	api.err = errBoom
	res := testRun(t, api, "status")
	if res.code != exitFailure || !strings.Contains(res.stderr, "kvorumctl: boom") {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	if !api.closed {
		t.Fatal("client should be closed after the command")
	}
}

func TestVersionCommand(t *testing.T) {
	t.Parallel()
	called := false
	var stdout, stderr bytes.Buffer
	e := &env{stdout: &stdout, stderr: &stderr, dial: func(client.Config) (kvorumAPI, error) {
		called = true
		return newFakeAPI(), nil
	}}
	if code := run([]string{"version"}, e); code != exitOK {
		t.Fatalf("exit code = %d", code)
	}
	if called {
		t.Fatal("version must not dial the cluster")
	}
	if !strings.Contains(stdout.String(), "go_version  go") {
		t.Fatalf("unexpected version output %q", stdout.String())
	}

	stdout.Reset()
	if code := run([]string{"--json", "version"}, e); code != exitOK {
		t.Fatalf("exit code = %d", code)
	}
	var v map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &v); err != nil {
		t.Fatalf("invalid json %q: %v", stdout.String(), err)
	}
	if v["version"] == "" || !strings.HasPrefix(v["go_version"], "go") {
		t.Fatalf("unexpected json %v", v)
	}
}

func TestSplitEndpoints(t *testing.T) {
	t.Parallel()
	got := splitEndpoints(" a:1 ,, b:2,")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("splitEndpoints = %q", got)
	}
	if got := splitEndpoints(" , "); got != nil {
		t.Fatalf("splitEndpoints(blank) = %q, want nil", got)
	}
}
