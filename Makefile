VERSION ?= dev
GIT_SHA ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
BUILT_AT ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS = -s -w -X github.com/ethan/smart-route/internal/buildinfo.Version=$(VERSION) -X github.com/ethan/smart-route/internal/buildinfo.GitSHA=$(GIT_SHA) -X github.com/ethan/smart-route/internal/buildinfo.BuiltAt=$(BUILT_AT)

.PHONY: build test verify images
build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" ./cmd/smart-route ./cmd/smart-route-worker
test:
	go test ./...
verify:
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))"
	go vet ./...
	go test ./...
	go test -race ./...
images:
	docker build --build-arg VERSION=$(VERSION) --build-arg GIT_SHA=$(GIT_SHA) --build-arg BUILT_AT=$(BUILT_AT) -t smart-route:$(VERSION) .
	docker build --build-arg VERSION=$(VERSION) --build-arg GIT_SHA=$(GIT_SHA) --build-arg BUILT_AT=$(BUILT_AT) -t smart-route-worker:$(VERSION) -f Dockerfile.worker .
