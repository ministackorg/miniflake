FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w" -o /miniflake ./cmd/miniflake

FROM alpine:3.20

RUN apk add --no-cache ca-certificates
COPY --from=builder /miniflake /usr/local/bin/miniflake

RUN mkdir -p /data /stages

EXPOSE 8084

ENTRYPOINT ["miniflake"]
CMD ["--data-dir", "/data", "--stage-dir", "/stages"]
