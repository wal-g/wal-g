CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard *.go)
TEST_FILES = $(wildcard walg_test/*.go testtools/*.go)

.PHONY: test fmt all install clean

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

test: build
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v ./test/
	go test -v ./internal/walparser/

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

all: build

deps:
	git submodule update --init
	dep ensure

install:
	(cd cmd/wal-g && go install)

clean:
	(cd cmd/wal-g && go clean)

build: $(CMD_FILES) $(PKG_FILES)
	(cd cmd/wal-g && go build $(GOTAGS) -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X main.GitRevision=`git rev-parse --short HEAD` -X main.WalgVersion=`git tag -l --points-at HEAD`")
