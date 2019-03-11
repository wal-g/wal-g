MAIN_PATH := main/wal-g
CMD_FILES = $(wildcard wal-g/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g

.PHONY: test fmt lint all install clean

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

test: build
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v ./test/
	go test -v ./internal/walparser/
	go test -v ./internal/storages/s3/
	go test -v ./internal/storages/gcs/
	go test -v ./internal/storages/fs/
	go test -v ./internal/storages/azure/
	go test -v ./internal/storages/swift/

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs golint

all: build

deps:
	git submodule update --init
	dep ensure

install:
	(cd $(MAIN_PATH) && go install)

clean:
	(cd $(MAIN_PATH) && go clean)

build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PATH) && go build $(GOTAGS) -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X main.GitRevision=`git rev-parse --short HEAD` -X main.WalgVersion=`git tag -l --points-at HEAD`")
