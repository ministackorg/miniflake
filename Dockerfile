FROM golang:1.26-bookworm AS builder

# go-duckdb links a prebuilt C++ libduckdb.a; Debian's stock toolchain
# (gcc + libstdc++ + dev symlinks) resolves -lstdc++ out of the box.
# Alpine's musl setup needs extra package juggling for the same result.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w" -o /miniflake ./cmd/miniflake \
    && mkdir -p /tmp/empty-data /tmp/empty-stages

# Distroless runtime: contains glibc + libstdc++ + ca-certificates and
# nothing else. No shell, no package manager, no busybox — the typical
# CVE-bearing surface is just gone. Image size ~25 MB vs ~85 MB for
# debian-slim. Trade-off: `docker exec sh` won't work; if you need to
# inspect a running container, use `:debug` tag (BusyBox) or run the
# binary against a debian-slim image one-off.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /miniflake /usr/local/bin/miniflake

# Distroless has no shell so `mkdir` doesn't run there — create the dirs
# in the builder stage and copy them over with the right ownership for the
# nonroot user (uid 65532).
COPY --from=builder --chown=65532:65532 /tmp/empty-data /data
COPY --from=builder --chown=65532:65532 /tmp/empty-stages /stages

EXPOSE 8084

ENTRYPOINT ["/usr/local/bin/miniflake"]
CMD ["--data-dir", "/data", "--stage-dir", "/stages"]
