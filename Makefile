MAIN_PG_PATH := main/pg
MAIN_MYSQL_PATH := main/mysql
MAIN_REDIS_PATH := main/redis
MAIN_MONGO_PATH := main/mongo
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

test: install deps lint unittest pg_build mysql_build redis_build mongo_build unlink_brotli pg_integration_test mysql_integration_test redis_integration_test mongo_integration_test

pg_test: install deps pg_build lint unittest unlink_brotli pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

pg_integration_test:
	docker-compose build $(DOCKER_COMMON) pg pg_tests
	docker-compose up --exit-code-from pg_tests pg_tests

pg_unittests: install deps lint unittest

pg_integration_tests_with_args: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg $(ARGS)
	docker-compose up --exit-code-from $(ARGS) $(ARGS)

pg_integration_delete_before_name_find_full_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_before_name_find_full_test
	docker-compose up --exit-code-from pg_delete_before_name_find_full_test pg_delete_before_name_find_full_test

pg_integration_delete_retain_full_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_retain_full_test
	docker-compose up --exit-code-from pg_delete_retain_full_test pg_delete_retain_full_test

pg_integration_full_backup_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_full_backup_test
	docker-compose up --exit-code-from pg_full_backup_test pg_full_backup_test

pg_integration_delete_before_time_find_full_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_before_time_find_full_test
	docker-compose up --exit-code-from pg_delete_before_time_find_full_test pg_delete_before_time_find_full_test

pg_integration_delete_without_confirm_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_without_confirm_test
	docker-compose up --exit-code-from pg_delete_without_confirm_test pg_delete_without_confirm_test

pg_integration_ghost_table_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_ghost_table_test
	docker-compose up --exit-code-from pg_ghost_table_test pg_ghost_table_test

pg_integration_config_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_config_test
	docker-compose up --exit-code-from pg_config_test pg_config_test

pg_integration_delete_end_to_end_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_end_to_end_test
	docker-compose up --exit-code-from pg_delete_end_to_end_test pg_delete_end_to_end_test

pg_integration_delta_backup_fullscan_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delta_backup_fullscan_test
	docker-compose up --exit-code-from pg_delta_backup_fullscan_test pg_delta_backup_fullscan_test

pg_integration_several_delta_backups_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_several_delta_backups_test
	docker-compose up --exit-code-from pg_several_delta_backups_test pg_several_delta_backups_test

pg_integration_crypto_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_crypto_test
	docker-compose up --exit-code-from pg_crypto_test pg_crypto_test

pg_integration_delete_retain_find_full_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delete_retain_find_full_test
	docker-compose up --exit-code-from pg_delete_retain_find_full_test pg_delete_retain_find_full_test

pg_integration_delta_backup_wal_delta_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_delta_backup_wal_delta_test
	docker-compose up --exit-code-from pg_delta_backup_wal_delta_test pg_delta_backup_wal_delta_test

pg_integration_wale_compatibility_test: install deps pg_build unlink_brotli
	docker-compose build $(DOCKER_COMMON) pg pg_wale_compatibility_test
	docker-compose up --exit-code-from pg_wale_compatibility_test pg_wale_compatibility_test

pg_int_tests_only:
	docker-compose build pg_tests
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

mongo_test: install deps mongo_build lint unittest unlink_brotli mongo_integration_test

mongo_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MONGO_PATH) && go build -o wal-g $(GOTAGS) -ldflags "-s -w -X github.com/wal-g/wal-g/cmd.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd.WalgVersion=`git tag -l --points-at HEAD`")

mongo_install: mongo_build
	mv $(MAIN_MONGO_PATH)/wal-g $(GOBIN)/wal-g

mongo_integration_test:
	docker-compose build $(DOCKER_COMMON) mongo mongo_tests
	docker-compose up --exit-code-from mongo_tests mongo_tests

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
