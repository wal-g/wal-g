MAIN_PG_PATH := main/pg
MAIN_MYSQL_PATH := main/mysql
MAIN_REDIS_PATH := main/redis
DOCKER_COMMON := golang ubuntu s3
CMD_FILES = $(wildcard wal-g/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g
COVERAGE_FILE := coverage.out

.PHONY: unittest fmt lint install clean

ifdef GOTAGS
override GOTAGS := -tags $(GOTAGS)
endif

test: install deps lint unittest pg_build mysql_build redis_build unlink_brotli pg_integration_test mysql_integration_test redis_integration_test

pg_test: install deps pg_build lint unittest unlink_brotli pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

pg_int_tests_only:
	docker-compose build pg_tests
	docker-compose up --exit-code-from pg_tests pg_tests

pg_integration_test:
	docker-compose build $(DOCKER_COMMON) pg pg_tests
	docker-compose up --exit-code-from pg_tests pg_tests

pg_clean:
	(cd $(MAIN_PG_PATH) && go clean)
	./cleanup.sh

pg_install: pg_build
	mv $(MAIN_PG_PATH)/wal-g $(GOBIN)/wal-g

mysql_test: install deps mysql_build lint unittest unlink_brotli mysql_integration_test

mysql_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MYSQL_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

mysql_integration_test:
	docker-compose build $(DOCKER_COMMON) mysql mysql_tests
	docker-compose up --exit-code-from mysql_tests mysql_tests

mysql_clean:
	(cd $(MAIN_MYSQL_PATH) && go clean)
	./cleanup.sh

mysql_install: mysql_build
	mv $(MAIN_MYSQL_PATH)/wal-g $(GOBIN)/wal-g

redis_test: install deps redis_build lint unittest unlink_brotli redis_integration_test

redis_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_REDIS_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

redis_integration_test:
	docker-compose build $(DOCKER_COMMON) redis redis_tests
	docker-compose up --exit-code-from redis_tests redis_tests

redis_clean:
	(cd $(MAIN_REDIS_PATH) && go clean)
	./cleanup.sh

redis_install: redis_build
	mv $(MAIN_REDIS_PATH)/wal-g $(GOBIN)/wal-g

unittest:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v $(TEST_MODIFIER) ./internal/
	go test -v $(TEST_MODIFIER) ./internal/compression/
	go test -v $(TEST_MODIFIER) ./internal/crypto/openpgp/
	go test -v $(TEST_MODIFIER) ./internal/crypto/awskms/
	go test -v $(TEST_MODIFIER) ./internal/databases/mysql
	go test -v $(TEST_MODIFIER) ./internal/storages/azure/
	go test -v $(TEST_MODIFIER) ./internal/storages/fs/
	go test -v $(TEST_MODIFIER) ./internal/storages/gcs/
	go test -v $(TEST_MODIFIER) ./internal/storages/s3/
	go test -v $(TEST_MODIFIER) ./internal/storages/storage
	go test -v $(TEST_MODIFIER) ./internal/storages/swift/
	go test -v $(TEST_MODIFIER) ./internal/walparser/
	go test -v $(TEST_MODIFIER) ./utility

coverage:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go test -v $(TEST_MODIFIER) -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs golint

deps:
	git submodule update --init
	dep ensure
	./link_brotli.sh

install:
	go get -u github.com/golang/dep/cmd/dep
	go get -u golang.org/x/lint/golint

unlink_brotli:
	rm -rf vendor/github.com/google/brotli/*
	mv tmp/* vendor/github.com/google/brotli/
	rm -rf tmp/
