FROM golang:1.26-alpine AS builder

# go-duckdb links libduckdb (a C++ library), so the build needs g++ (for
# libstdc++/-lstdc++) plus the system header that comes with linux-headers.
# `gcc` + `musl-dev` alone is not enough — the linker fails with
# `cannot find -lstdc++`.
RUN apk add --no-cache gcc g++ musl-dev linux-headers

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w" -o /miniflake ./cmd/miniflake

FROM alpine:3.20

# libstdc++ + libgcc are required at runtime since go-duckdb is dynamically
# linked against them (the alpine builder produced a musl-libc binary that
# still depends on the C++ runtime).
RUN apk add --no-cache ca-certificates libstdc++ libgcc
COPY --from=builder /miniflake /usr/local/bin/miniflake

RUN mkdir -p /data /stages

EXPOSE 8084

ENTRYPOINT ["miniflake"]
CMD ["--data-dir", "/data", "--stage-dir", "/stages"]
