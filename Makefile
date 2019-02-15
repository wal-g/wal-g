CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g

.PHONY: test fmt lint all install clean alpine

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

test: build
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v ./test/
	go test -v ./internal/walparser/

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs golint

all: build

deps:
	git submodule update --init
	dep ensure -v

install:
	(cd cmd/wal-g && go install)

clean:
	(cd cmd/wal-g && go clean)

build: $(CMD_FILES) $(PKG_FILES)
	(cd cmd/wal-g && go build $(GOTAGS) -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X main.GitRevision=`git rev-parse --short HEAD` -X main.WalgVersion=`git tag -l --points-at HEAD`")

alpine: $(CMD_FILES) $(PKG_FILES)
	rm -rf .brotli.tmp
	rm -rf ./vendor/github.com/google/brotli/dist
	docker build --pull -t wal-g/golang:1.11-alpine ./docker/go-alpine
	docker run                          \
	    --rm                            \
	    -u $$(id -u):$$(id -g)          \
	    -v /tmp:/.cache                 \
	    -v /tmp:/go/src/github.com/golang            \
	    -v "$$(pwd):/go/src/$(PKG)"     \
	    -w /go/src/$(PKG)               \
	    -e GOOS=linux                   \
	    -e GOARCH=amd64                 \
	    wal-g/golang:1.11-alpine        \
	    ./build-alpine.sh
