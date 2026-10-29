# kvorum build and development tasks.

MODULE      := github.com/sadrax4/kvorum
BIN_DIR     := bin
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
	-X $(MODULE)/internal/telemetry.Version=$(VERSION) \
	-X $(MODULE)/internal/telemetry.Commit=$(COMMIT) \
	-X $(MODULE)/internal/telemetry.BuildDate=$(BUILD_DATE)

GO          ?= go
GOFLAGS     ?=
PKGS        := ./...
SIM_SEEDS   ?= 200

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the server and CLI into ./bin.
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/kvorum ./cmd/kvorum
	$(GO) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/kvorumctl ./cmd/kvorumctl

.PHONY: install
install: ## Install the binaries with go install.
	$(GO) install -trimpath -ldflags '$(LDFLAGS)' ./cmd/...

.PHONY: test
test: ## Run unit and integration tests with the race detector.
	$(GO) test $(GOFLAGS) -race -timeout 10m $(PKGS)

.PHONY: test-short
test-short: ## Run fast tests only.
	$(GO) test $(GOFLAGS) -short -timeout 5m $(PKGS)

.PHONY: test-sim
test-sim: ## Run the Raft simulator with many seeds (SIM_SEEDS=N).
	$(GO) test $(GOFLAGS) -race -timeout 30m -run 'TestSim' -seeds $(SIM_SEEDS) ./raft/

.PHONY: cover
cover: ## Produce coverage.out and print a summary.
	$(GO) test $(GOFLAGS) -race -timeout 10m -coverprofile=coverage.out -covermode=atomic $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: bench
bench: ## Run benchmarks.
	$(GO) test $(GOFLAGS) -run '^$$' -bench . -benchmem $(PKGS)

.PHONY: lint
lint: ## Run golangci-lint and buf lint.
	golangci-lint run ./...
	buf lint

.PHONY: fmt
fmt: ## Format Go sources.
	gofmt -s -w $$(git ls-files '*.go' | grep -v '\.pb\.go$$')

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet $(PKGS)

.PHONY: proto
proto: ## Regenerate protobuf and gRPC code with buf.
	buf generate

.PHONY: proto-check
proto-check: proto ## Fail if generated code is out of date.
	@git diff --exit-code -- api/ || (echo "generated code is stale: run 'make proto'" && exit 1)

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum.
	$(GO) mod tidy

.PHONY: docker
docker: ## Build the container image.
	docker build -t kvorum:$(VERSION) -t kvorum:latest .

.PHONY: cluster-up
cluster-up: ## Start a three-node cluster with Docker Compose.
	docker compose -f deploy/docker-compose.yml up --build -d

.PHONY: cluster-down
cluster-down: ## Stop the Docker Compose cluster and remove its volumes.
	docker compose -f deploy/docker-compose.yml down -v

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR) coverage.out data
