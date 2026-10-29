package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
)

// errHelpRequested is returned when -h or --help is given.
var errHelpRequested = flag.ErrHelp

// exitError carries an exit code and the message to print before exiting.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

// usageErrorf builds an error that exits with status 2 after printing msg and
// the usage of the command.
func usageErrorf(format string, args ...any) error {
	return &exitError{code: exitUsage, msg: fmt.Sprintf(format, args...)}
}

// silentExit builds an error that exits with the given status and prints
// nothing; the command has already reported the outcome.
func silentExit(code int) error {
	return &exitError{code: code}
}

// newFlagSet creates a flag set that reports errors through its return value
// and prints nothing by itself.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// parseInterleaved parses fs over args, allowing flags before, between and
// after positional arguments. Everything after a bare "--" is positional. It
// returns the positional arguments in order; a flag error becomes a usage
// error and -h is passed through as errHelpRequested.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for len(args) > 0 {
		if args[0] == "--" {
			positional = append(positional, args[1:]...)
			break
		}
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, errHelpRequested
			}
			return nil, usageErrorf("%v", err)
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		consumed := len(args) - len(rest)
		if consumed > 0 && args[consumed-1] == "--" {
			positional = append(positional, rest...)
			break
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
	return positional, nil
}

// wantArgs checks that exactly the named positional arguments were given.
func wantArgs(positional []string, names ...string) error {
	if len(positional) < len(names) {
		return usageErrorf("missing argument <%s>", names[len(positional)])
	}
	if len(positional) > len(names) {
		return usageErrorf("unexpected argument %q", positional[len(names)])
	}
	return nil
}

// parseID parses a member id, which must be a positive integer.
func parseID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, usageErrorf("invalid id %q: must be an unsigned integer", s)
	}
	if id == 0 {
		return 0, usageErrorf("invalid id %q: must be greater than zero", s)
	}
	return id, nil
}
