MAIN_PG_PATH := main/pg
CMD_FILES = $(wildcard wal-g/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g

.PHONY: unittest fmt lint install clean

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

pg_test: install deps pg_build lint unittest pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

pg_integration_test:
	rm -rf vendor/github.com/google/brotli/*
	mv tmp/* vendor/github.com/google/brotli/
	rm -rf tmp/
	docker-compose build
	docker-compose up --exit-code-from pg pg

pg_clean:
	(cd $(MAIN_PG_PATH) && go clean)
	./cleanup.sh

unittest:
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

deps:
	go get -u github.com/golang/dep/cmd/dep
	git submodule update --init
	dep ensure
	./link_brotli.sh

install:
	go get -u github.com/golang/dep/cmd/dep
	go get -u golang.org/x/lint/golint