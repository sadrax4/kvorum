// Package telemetry holds build information, structured logging setup, and the
// Prometheus metrics shared by the kvorum server components.
package telemetry

import "runtime"

// Build information, injected at link time by the Makefile via -ldflags.
var (
	// Version is the semantic version or git describe output of the build.
	Version = "dev"
	// Commit is the git commit the binary was built from.
	Commit = "unknown"
	// BuildDate is the UTC build timestamp in RFC 3339 format.
	BuildDate = "unknown"
)

// GoVersion returns the Go toolchain version the binary was built with.
func GoVersion() string {
	return runtime.Version()
}
