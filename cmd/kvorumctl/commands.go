package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// commandFunc is the body of a command. It writes its output to out and
// reports failures as errors; an *exitError selects the exit code.
type commandFunc func(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error

// command describes one subcommand.
type command struct {
	name        string
	synopsis    string
	summary     string
	flagsHelp   []string
	needsClient bool
	run         commandFunc
}

// commands lists every subcommand in the order they appear in the usage text.
func commands() []command {
	return []command{
		{name: "get", synopsis: "<key> [--stale]", summary: "Read a key.",
			flagsHelp:   []string{"--stale  serve the read from the local replica without coordination"},
			needsClient: true, run: cmdGet},
		{name: "put", synopsis: "<key> <value> [--prev-kv]", summary: "Write a key.",
			flagsHelp:   []string{"--prev-kv  return the previous value"},
			needsClient: true, run: cmdPut},
		{name: "del", synopsis: "<key> [--prev-kv]", summary: "Delete a key.",
			flagsHelp:   []string{"--prev-kv  return the deleted value"},
			needsClient: true, run: cmdDel},
		{name: "cas", synopsis: "<key> <expected-version> <value>", summary: "Compare-and-swap a key; exits 1 when the version does not match.",
			needsClient: true, run: cmdCAS},
		{name: "scan", synopsis: "[--prefix P | --start S --end E] [--limit N] [--stale]", summary: "List keys in a range.",
			flagsHelp: []string{
				"--prefix P  keys starting with P (exclusive with --start/--end)",
				"--start S   inclusive start of the range (default: smallest key)",
				"--end E     exclusive end of the range (default: unbounded)",
				"--limit N   return at most N keys",
				"--stale     serve the read from the local replica without coordination",
			},
			needsClient: true, run: cmdScan},
		{name: "watch", synopsis: "<key> [--prefix]", summary: "Stream changes to a key or prefix until interrupted.",
			flagsHelp:   []string{"--prefix  watch every key starting with <key>"},
			needsClient: true, run: cmdWatch},
		{name: "status", summary: "Show the Raft status of the serving node.", needsClient: true, run: cmdStatus},
		{name: "members", summary: "List cluster members.", needsClient: true, run: cmdMembers},
		{name: "member", synopsis: "add <id> <raft-addr> [client-addr] [--learner] | remove <id> | promote <id>",
			summary:     "Change cluster membership.",
			flagsHelp:   []string{"--learner  (add) add the node as a non-voting learner"},
			needsClient: true, run: cmdMember},
		{name: "transfer-leader", synopsis: "<id>", summary: "Transfer leadership to a member.", needsClient: true, run: cmdTransferLeader},
		{name: "snapshot", summary: "Force a snapshot on the serving node.", needsClient: true, run: cmdSnapshot},
		{name: "bench", synopsis: "[--op put|get|mixed] [--clients N] [--total N] [--key-space N] [--value-size BYTES]",
			summary: "Run a load test and report throughput and latency percentiles.",
			flagsHelp: []string{
				"--op OP           put, get or mixed (default put)",
				"--clients N       concurrent clients (default 8)",
				"--total N         total operations (default 10000)",
				"--key-space N     number of distinct keys (default 1000)",
				"--value-size N    value size in bytes (default 128)",
			},
			needsClient: true, run: cmdBench},
		{name: "version", summary: "Print build information.", run: cmdVersion},
	}
}

func lookupCommand(name string) (command, bool) {
	for _, c := range commands() {
		if c.name == name {
			return c, true
		}
	}
	return command{}, false
}

// printUsage writes the top-level usage text. Usage text is best effort: a
// failing writer cannot be reported anywhere, so write errors are ignored.
func printUsage(w io.Writer) {
	var b bytes.Buffer
	fmt.Fprintln(&b, "Usage: kvorumctl [global flags] <command> [arguments] [command flags]")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Global flags (before the command):")
	fmt.Fprintf(&b, "  --endpoints LIST  comma-separated client addresses (env %s, default %s)\n", endpointsEnvVar, defaultEndpoints)
	fmt.Fprintf(&b, "  --timeout D       per-request timeout (default %s)\n", defaultTimeout)
	fmt.Fprintln(&b, "  --json            print machine-readable JSON instead of text")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Commands:")
	type row struct{ left, right string }
	rows := make([]row, 0, len(commands())+1)
	width := 0
	for _, c := range commands() {
		rows = append(rows, row{left: c.name, right: c.summary})
		width = max(width, len(c.name))
	}
	rows = append(rows, row{left: "help [command]", right: "Show this text or the help of one command."})
	width = max(width, len("help [command]"))
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, r.left, r.right)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Run \"kvorumctl help <command>\" for the arguments and flags of a command.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Exit status: 0 on success, 1 on error, 2 on a usage error.")
	_, _ = w.Write(b.Bytes())
}

// printCommandUsage writes the usage text of one command.
func printCommandUsage(w io.Writer, c command) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "Usage: kvorumctl [global flags] %s %s\n", c.name, c.synopsis)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, c.summary)
	if len(c.flagsHelp) > 0 {
		fmt.Fprintln(&b)
		fmt.Fprintln(&b, "Flags:")
		for _, line := range c.flagsHelp {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	_, _ = w.Write(b.Bytes())
}
