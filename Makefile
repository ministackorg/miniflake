VERSION ?= 0.1.5
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT)"

.PHONY: build run test clean docker

build:
	go build $(LDFLAGS) -o bin/miniflake ./cmd/miniflake

run: build
	./bin/miniflake

test:
	go test ./... -v -race -count=1

test-integration:
	go test ./test/integration/... -v -race -count=1 -tags=integration

clean:
	rm -rf bin/ data/ stages/

docker:
	docker build -t miniflakedb/miniflake:$(VERSION) -t miniflakedb/miniflake:latest .

fmt:
	gofmt -w .
	go vet ./...
