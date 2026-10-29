# Contributing

Thanks for your interest in kvorum. This document explains how to set up a
development environment and what is expected of a change.

## Prerequisites

- Go 1.26 or newer
- [buf](https://buf.build) and the protoc plugins (`go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2`) if you change `.proto` files
- [golangci-lint](https://golangci-lint.run) v2
- Docker (optional, for the Compose cluster)

## Everyday commands

| Command | What it does |
| --- | --- |
| `make build` | Builds `bin/kvorum` and `bin/kvorumctl` |
| `make test` | Runs every test with the race detector |
| `make test-sim SIM_SEEDS=500` | Runs the Raft simulator with more seeds |
| `make lint` | golangci-lint and buf lint |
| `make proto` | Regenerates the protobuf and gRPC code |
| `make cluster-up` / `make cluster-down` | Three-node Docker Compose cluster |
| `scripts/run-local-cluster.sh` | Three-node cluster as local processes |

## Testing expectations

- Every change to `raft/` must keep the simulator green. Run it with at least
  200 seeds before opening a pull request; a failing seed is reproducible with
  `go test ./raft -run TestSimulation -sim.seed=N -v`.
- Durability changes (`internal/wal`) need a crash-recovery test.
- New API behaviour needs an integration test in `test/integration`.
- Tests must pass under `-race` and must not depend on wall-clock timing
  beyond generous timeouts.

## Style

- Idiomatic Go, `gofmt`-formatted, lint-clean.
- Doc comments on every exported identifier; package doc comments that
  explain the design, not just the API.
- Errors are wrapped with `%w`; sentinel errors exist where callers branch.
- Structured logging through `log/slog`.

## Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org): `feat:`,
  `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
- Keep pull requests focused. Describe the problem, the approach, and how you
  tested it; the pull request template asks for exactly that.
- Design changes should come with an ADR in `docs/adr/`.
