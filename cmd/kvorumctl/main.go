// Command kvorumctl is the command-line client for a kvorum cluster.
//
// Usage:
//
//	kvorumctl [global flags] <command> [arguments] [command flags]
//
// Global flags come before the command. Run "kvorumctl help" for the list of
// commands and "kvorumctl <command> -h" for the flags of one command.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sadrax4/kvorum/client"
)

// Exit codes.
const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

const (
	defaultEndpoints = "127.0.0.1:2379"
	defaultTimeout   = 5 * time.Second
	endpointsEnvVar  = "KVORUM_ENDPOINTS"
)

// globals holds the global flags shared by every command.
type globals struct {
	endpoints []string
	timeout   time.Duration
	json      bool
}

// env is the process environment a run uses. Tests substitute every field.
type env struct {
	stdout io.Writer
	stderr io.Writer
	getenv func(string) string
	// dial creates the client every command except version talks through.
	dial func(cfg client.Config) (kvorumAPI, error)
	// ctx is the base context of the run; it ends on an interrupt signal.
	ctx context.Context
}

// errorf writes a diagnostic to stderr. Diagnostics are best effort: a
// failing stderr cannot be reported anywhere.
func (e *env) errorf(format string, args ...any) {
	_, _ = fmt.Fprintf(e.stderr, format, args...)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(os.Args[1:], &env{
		stdout: os.Stdout,
		stderr: os.Stderr,
		getenv: os.Getenv,
		dial:   dialClient,
		ctx:    ctx,
	})
	stop()
	os.Exit(code)
}

func dialClient(cfg client.Config) (kvorumAPI, error) {
	return client.New(cfg)
}

// run executes one invocation and returns its exit code. It never panics on
// user input and writes nothing outside e.stdout and e.stderr.
func run(args []string, e *env) int {
	g, rest, err := parseGlobals(args, e)
	if err != nil {
		if errors.Is(err, errHelpRequested) {
			printUsage(e.stdout)
			return exitOK
		}
		e.errorf("kvorumctl: %v\n", err)
		printUsage(e.stderr)
		return exitUsage
	}
	if len(rest) == 0 {
		printUsage(e.stderr)
		return exitUsage
	}
	name, cmdArgs := rest[0], rest[1:]
	if name == "help" {
		return runHelp(cmdArgs, e)
	}
	cmd, ok := lookupCommand(name)
	if !ok {
		e.errorf("kvorumctl: unknown command %q\n", name)
		printUsage(e.stderr)
		return exitUsage
	}
	return runCommand(cmd, cmdArgs, g, e)
}

func runCommand(cmd command, args []string, g globals, e *env) int {
	ctx := e.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var api kvorumAPI
	if cmd.needsClient {
		var err error
		api, err = e.dial(client.Config{
			Endpoints:      g.endpoints,
			DialTimeout:    g.timeout,
			RequestTimeout: g.timeout,
		})
		if err != nil {
			e.errorf("kvorumctl: %v\n", err)
			return exitFailure
		}
		defer func() { _ = api.Close() }()
	}
	err := cmd.run(ctx, api, g, args, e.stdout)
	return reportError(err, cmd, e)
}

// reportError turns the error of a command into an exit code and the matching
// message on stderr.
func reportError(err error, cmd command, e *env) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, errHelpRequested) {
		printCommandUsage(e.stdout, cmd)
		return exitOK
	}
	var ee *exitError
	if errors.As(err, &ee) {
		if ee.msg != "" {
			e.errorf("kvorumctl: %s\n", ee.msg)
		}
		if ee.code == exitUsage {
			printCommandUsage(e.stderr, cmd)
		}
		return ee.code
	}
	e.errorf("kvorumctl: %v\n", err)
	return exitFailure
}

func runHelp(args []string, e *env) int {
	if len(args) == 0 {
		printUsage(e.stdout)
		return exitOK
	}
	cmd, ok := lookupCommand(args[0])
	if !ok {
		e.errorf("kvorumctl: unknown command %q\n", args[0])
		printUsage(e.stderr)
		return exitUsage
	}
	printCommandUsage(e.stdout, cmd)
	return exitOK
}

// parseGlobals parses the global flags that precede the command name.
func parseGlobals(args []string, e *env) (globals, []string, error) {
	getenv := e.getenv
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	defaultEP := getenv(endpointsEnvVar)
	if strings.TrimSpace(defaultEP) == "" {
		defaultEP = defaultEndpoints
	}
	fs := newFlagSet("kvorumctl")
	endpoints := fs.String("endpoints", defaultEP, "comma-separated client addresses (env "+endpointsEnvVar+")")
	timeout := fs.Duration("timeout", defaultTimeout, "per-request timeout")
	jsonOut := fs.Bool("json", false, "print machine-readable JSON instead of text")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, errHelpRequested) {
			return globals{}, nil, errHelpRequested
		}
		return globals{}, nil, err
	}
	g := globals{endpoints: splitEndpoints(*endpoints), timeout: *timeout, json: *jsonOut}
	if len(g.endpoints) == 0 {
		return globals{}, nil, errors.New("no endpoints given")
	}
	if g.timeout <= 0 {
		return globals{}, nil, errors.New("--timeout must be positive")
	}
	return g, fs.Args(), nil
}

func splitEndpoints(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
