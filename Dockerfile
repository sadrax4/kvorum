# syntax=docker/dockerfile:1

# Build stage: compile static binaries with build information baked in.
FROM golang:1.26 AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w \
      -X github.com/sadrax4/kvorum/internal/telemetry.Version=${VERSION} \
      -X github.com/sadrax4/kvorum/internal/telemetry.Commit=${COMMIT} \
      -X github.com/sadrax4/kvorum/internal/telemetry.BuildDate=${BUILD_DATE}" \
      -o /out/kvorum ./cmd/kvorum && \
    go build -ldflags "-s -w \
      -X github.com/sadrax4/kvorum/internal/telemetry.Version=${VERSION} \
      -X github.com/sadrax4/kvorum/internal/telemetry.Commit=${COMMIT} \
      -X github.com/sadrax4/kvorum/internal/telemetry.BuildDate=${BUILD_DATE}" \
      -o /out/kvorumctl ./cmd/kvorumctl

# Runtime stage: no shell, no package manager, non-root user.
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev

# OCI labels link the image to the repository on GHCR and carry provenance.
LABEL org.opencontainers.image.title="kvorum" \
      org.opencontainers.image.description="Distributed, strongly consistent key-value store on a from-scratch Raft implementation." \
      org.opencontainers.image.source="https://github.com/sadrax4/kvorum" \
      org.opencontainers.image.url="https://github.com/sadrax4/kvorum" \
      org.opencontainers.image.documentation="https://github.com/sadrax4/kvorum/blob/main/docs/operations.md" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build /out/kvorum /usr/local/bin/kvorum
COPY --from=build /out/kvorumctl /usr/local/bin/kvorumctl

EXPOSE 2379 2380 2381
VOLUME ["/data"]
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/kvorum"]
CMD ["--data-dir", "/data"]
