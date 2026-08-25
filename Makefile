MAIN_PG_PATH := main/pg
MAIN_MYSQL_PATH := main/mysql
MAIN_SQLSERVER_PATH := main/sqlserver
MAIN_REDIS_PATH := main/redis
MAIN_MONGO_PATH := main/mongo
MAIN_FDB_PATH := main/fdb
MAIN_GP_PATH := main/gp
MAIN_ETCD_PATH := main/etcd
DOCKER_COMMON := golang ubuntu ubuntu_22_04 s3
CMD_FILES = $(wildcard cmd/**/*.go)
PKG_FILES = $(wildcard internal/*.go internal/**/*.go internal/**/**/*.go internal/**/**/**/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g
COVERAGE_FILE := coverage.out
TEST := "pg_tests"
# PostgreSQL version the test services run on. 10 uses the old bionic image (the
# only one that has wal-e); 14-18 use the shared PGDG image.
PG_MAJOR ?= 10
export PG_MAJOR
# pgBackRest is built from source and added to the PG test image, so it must be
# built on the same Ubuntu version as that image. 2.54 and later need meson,
# which is too new for bionic, so PG 10 uses the older 2.36.
ifeq ($(PG_MAJOR),10)
PGBACKREST_BUILD_BASE := ubuntu:18.04
PGBACKREST_VERSION    := 2.36
else
PGBACKREST_BUILD_BASE := ubuntu:22.04
PGBACKREST_VERSION    := 2.59.0
endif
export PGBACKREST_BUILD_BASE PGBACKREST_VERSION
MYSQL_TEST := "mysql_base_tests"
MYSQL8_TEST := "mysql8_tests"
MONGO_VERSION ?= "8.0.3"
MONGO_PACKAGE ?= "mongodb-org"
MONGO_REPO ?= "repo.mongodb.org"
MONGO_TEST_TYPE ?= "all"
GOLANGCI_LINT_VERSION ?= "v2.4.0"
REDIS_VERSION ?= "6.2.4"
MOCKS_DESTINATION := ./testtools/mocks
FILE_TO_MOCKS := ./internal/uploader.go # list interface paths here
WALG_VERSION ?= `git tag -l --points-at HEAD | tail -1`
GIT_REVISION ?= `git rev-parse --short HEAD`

BUILD_TAGS:=

ifdef USE_BROTLI
	BUILD_TAGS:=$(BUILD_TAGS) brotli
endif

ifdef USE_LIBSODIUM
	BUILD_TAGS:=$(BUILD_TAGS) libsodium
endif

ifdef USE_LZO
	BUILD_TAGS:=$(BUILD_TAGS) lzo
endif

BUILD_GCFLAGS := 

ifdef ENABLE_DEBUG
	BUILD_GCFLAGS:=$(BUILD_GCFLAGS) all=-N -l
endif

STRIP_BINARIES ?= 1
BUILD_LDFLAGS :=

ifeq ($(STRIP_BINARIES),1)
	BUILD_LDFLAGS += -s -w
endif

.PHONY: unittest fmt lint clean

test: deps unittest pg_build mysql_build redis_build mongo_build gp_build cloudberry_build unlink_brotli pg_integration_test mysql_integration_test redis_integration_test fdb_integration_test gp_integration_test cloudberry_integration_test etcd_integration_test

pg_test: deps pg_build unlink_brotli pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/pg.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/pg.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/pg.walgVersion=$(WALG_VERSION)")

install_and_build_pg: deps pg_build

pg10_build_image: go_deps
ifeq ($(COMPOSE_BAKE),true)
	# bake resolves DAG across services in one invocation via additional_contexts (see docker-compose.bake.yml).
	docker compose build $(DOCKER_COMMON) pg10 pg10_tests_template
else
	# There are dependencies between container images.
	# Running in one command leads to using outdated images and fails on clean system.
	# It can not be fixed with depends_on in compose file. https://github.com/docker/compose/issues/6332
	docker compose build $(DOCKER_COMMON)
	docker compose build pg10
	docker compose build pg10_tests_template
endif

# Builds the image for any PostgreSQL 14-18, chosen by PG_MAJOR (ubuntu 22.04 and
# PGDG). PG 10 still uses pg10_build_image above, because wal-e needs python3.7
# and only the bionic image has it.
pg_build_image: go_deps
ifeq ($(PG_MAJOR),10)
	$(error PG_MAJOR=10 must be built with pg10_build_image - the PG 10 image is bionic + wal-e)
endif
ifeq ($(COMPOSE_BAKE),true)
	# bake resolves DAG across services in one invocation via additional_contexts (see docker-compose.bake.yml).
	docker compose build $(DOCKER_COMMON) pg pg_tests_template
else
	# pg_tests_template is built FROM wal-g/pg$(PG_MAJOR). In a single command
	# compose would look for that image on docker.io instead of using the one it
	# is about to build. https://github.com/docker/compose/issues/6332
	docker compose build $(DOCKER_COMMON)
	docker compose build pg
	docker compose build pg_tests_template
endif

# PG 10 is built by pg10_build_image (bionic + wal-e), the rest by pg_build_image.
# This list is also what CI tests: dockertests-par.yml reads it from here.
PG_VERSIONS ?= 10 14 15 16 17 18

.PHONY: print_pg_versions
print_pg_versions:
	@echo $(PG_VERSIONS)

# Run the whole suite sequentially for each version in PG_VERSIONS.
#   make pg_matrix_test                       # all supported versions
#   make PG_VERSIONS="17" pg_matrix_test      # just one
#   make PG_VERSIONS="10 18" pg_matrix_test   # the edges
pg_matrix_test:
	@for v in $(PG_VERSIONS); do \
		echo "=============== PostgreSQL $$v ==============="; \
		$(MAKE) PG_MAJOR=$$v pg_integration_test || exit 1; \
	done

pg_save_image: install_and_build_pg pg10_build_image
	mkdir -p ${CACHE_FOLDER}
	sudo rm -rf ${CACHE_FOLDER}/*
	for v in $(PG_VERSIONS); do \
		if [ "$$v" != "10" ]; then $(MAKE) PG_MAJOR=$$v pg_build_image; fi; \
		docker save wal-g/pg$${v}_tests > ${CACHE_FOLDER}/pg$${v}_tests.tar; \
	done
	docker save wal-g/ubuntu:18.04 > ${CACHE_FILE_UBUNTU_18_04}
	docker save wal-g/ubuntu:22.04 > ${CACHE_FILE_UBUNTU_22_04}
	docker save ${IMAGE_GOLANG}    > ${CACHE_FILE_GOLANG}
	ls -la ${CACHE_FOLDER}

pg_integration_test: clean_compose
	@tar="${CACHE_FOLDER}/pg$(PG_MAJOR)_tests.tar";\
	if [ -n "${CACHE_FOLDER}" ] && [ -f "$$tar" ]; then\
		docker load -i "$$tar" && rm "$$tar";\
	else\
		echo "No cached image for PG $(PG_MAJOR), building";\
		make install_and_build_pg;\
		if [ "$(PG_MAJOR)" = "10" ]; then\
			make pg10_build_image;\
		else\
			make PG_MAJOR=$(PG_MAJOR) pg_build_image;\
		fi;\
	fi
	@if echo "$(TEST)" | grep -Fqe "pgbackrest"; then\
		docker compose build pg_pgbackrest;\
	fi
	@if echo "$(TEST)" | grep -Fq -e "ssh_"; then\
		docker compose build ssh;\
	fi

	docker compose up --exit-code-from $(TEST) $(TEST)
	# Run tests with dependencies if we run all tests
	@if [ "$(TEST)" = "pg_tests" ]; then\
		docker compose build pg_pgbackrest ssh swift pg_wal_perftest_with_throttling &&\
		docker compose up --exit-code-from pg_ssh_backup_test pg_ssh_backup_test &&\
		docker compose up --exit-code-from pg_storage_swift_test pg_storage_swift_test &&\
		docker compose up --exit-code-from pg_storage_ssh_test pg_storage_ssh_test &&\
		docker compose up --exit-code-from pg_pgbackrest_backup_fetch_test pg_pgbackrest_backup_fetch_test &&\
		docker compose down &&\
		docker compose up --exit-code-from pg_wal_perftest_with_throttling pg_wal_perftest_with_throttling ;\
	fi
	make clean_compose

orioledb_integration_test: install_and_build_pg clean_compose load_docker_common
	docker compose build orioledb
	docker compose up --exit-code-from orioledb orioledb
	make clean_compose

.PHONY: clean_compose
clean_compose:
	services=$$(docker compose ps -a --format '{{.Name}} {{.Service}}' | grep wal-g_ | cut -d' ' -f 2); \
		if [ "$$services" ]; then docker compose down $$services; fi

all_unittests: deps unittest

# todo Should we remove this target as a duplicate of pg_integration_test?
pg_int_tests_only:
	docker compose build pg_tests
	docker compose up --exit-code-from pg_tests pg_tests

pg_clean:
	(cd $(MAIN_PG_PATH) && go clean)
	./cleanup.sh

pg_install: pg_build
	mv $(MAIN_PG_PATH)/wal-g $(GOBIN)/wal-g

mysql_base: deps mysql_build unlink_brotli
mysql_test: deps mysql_build unlink_brotli mysql_integration_test

mysql_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MYSQL_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/mysql.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mysql.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/mysql.walgVersion=$(WALG_VERSION)")

sqlserver_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_SQLSERVER_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/sqlserver.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/sqlserver.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/sqlserver.walgVersion=$(WALG_VERSION)")

load_docker_common:
	@if [ "x" = "${CACHE_FOLDER}x" ]; then\
		echo "Rebuild";\
		docker compose build $(DOCKER_COMMON);\
	else\
		docker load -i ${CACHE_FILE_UBUNTU_18_04} && rm ${CACHE_FILE_UBUNTU_18_04};\
		docker load -i ${CACHE_FILE_UBUNTU_22_04} && rm ${CACHE_FILE_UBUNTU_22_04};\
		docker load -i ${CACHE_FILE_GOLANG} && rm ${CACHE_FILE_GOLANG};\
	fi

mysql_integration_test: deps mysql_build unlink_brotli load_docker_common
	./link_brotli.sh
	docker compose build mysql && docker compose build $(MYSQL_TEST)
	docker compose up --force-recreate --exit-code-from $(MYSQL_TEST) $(MYSQL_TEST)

mysql8_integration_test: go_deps unlink_brotli load_docker_common
	docker compose build mysql8 && docker compose build $(MYSQL8_TEST)
	docker compose up --force-recreate --exit-code-from $(MYSQL8_TEST) $(MYSQL8_TEST)

mysql_clean:
	(cd $(MAIN_MYSQL_PATH) && go clean)
	./cleanup.sh

mysql_install: mysql_build
	mv $(MAIN_MYSQL_PATH)/wal-g $(GOBIN)/wal-g

mariadb_test: deps mysql_build unlink_brotli mariadb_integration_test

mariadb_integration_test: unlink_brotli load_docker_common
	./link_brotli.sh
	docker compose build mariadb && docker compose build mariadb_tests
	docker compose up --force-recreate --exit-code-from mariadb_tests mariadb_tests

mongo_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MONGO_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/mongo.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mongo.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/mongo.walgVersion=$(WALG_VERSION)")

mongo_install: mongo_build
	mv $(MAIN_MONGO_PATH)/wal-g $(GOBIN)/wal-g

mongo_features:
	set -e
	make go_deps
	cd tests_func/ && MONGO_VERSION=$(MONGO_VERSION) MONGO_PACKAGE=$(MONGO_PACKAGE) MONGO_REPO=$(MONGO_REPO) MONGO_TEST_TYPE=$(MONGO_TEST_TYPE) go test -v -count=1 -timeout 45m  --tf.test=true --tf.debug=true --tf.clean=false --tf.stop=false --tf.database=mongodb

mongo_binary_features:
	MONGO_TEST_TYPE="binary" $(MAKE) mongo_features

mongo_logical_features:
	MONGO_TEST_TYPE="logical" $(MAKE) mongo_features

mongo_partial_features:
	MONGO_TEST_TYPE="partial" $(MAKE) mongo_features

mongo_catch_up_features:
	MONGO_TEST_TYPE="catch_up" $(MAKE) mongo_features

clean_mongo_features:
	set -e
	cd tests_func/ && MONGO_VERSION=$(MONGO_VERSION) MONGO_PACKAGE=$(MONGO_PACKAGE) MONGO_REPO=$(MONGO_REPO) go test -v -count=1  -timeout 5m --tf.test=false --tf.debug=false --tf.clean=true --tf.stop=true --tf.database=mongodb

fdb_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_FDB_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS)")

fdb_install: fdb_build
	mv $(MAIN_FDB_PATH)/wal-g $(GOBIN)/wal-g

fdb_integration_test: load_docker_common
	docker compose down -v
	docker compose build fdb_tests
	docker compose up --force-recreate --renew-anon-volumes --exit-code-from fdb_tests fdb_tests

# Redis integration tests build a Brotli-enabled test image. Keep the Brotli
# sources and static libraries linked until that image build finishes.
redis_test:
	@set -e; \
	$(MAKE) USE_BROTLI=1 deps redis_build redis_integration_test; \
	$(MAKE) USE_BROTLI=1 unlink_brotli

redis_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_REDIS_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/redis.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/redis.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/redis.walgVersion=$(WALG_VERSION)")

redis_integration_test: load_docker_common
	docker compose build redis && docker compose build redis_tests
	docker compose up --exit-code-from redis_tests redis_tests

redis_clean:
	(cd $(MAIN_REDIS_PATH) && go clean)
	./cleanup.sh

redis_install: redis_build
	mv $(MAIN_REDIS_PATH)/wal-g $(GOBIN)/wal-g

redis_features:
	set -e
	make go_deps
	cd tests_func/ && FEATURE=$(FEATURE) REDIS_VERSION=$(REDIS_VERSION) go test -v -count=1 -timeout 20m  --tf.test=true --tf.debug=false --tf.clean=false --tf.stop=false --tf.database=redis

clean_redis_features:
	set -e
	cd tests_func/ && FEATURE=$(FEATURE) REDIS_VERSION=$(REDIS_VERSION) go test -v -count=1  -timeout 5m --tf.test=false --tf.debug=false --tf.clean=true --tf.stop=true --tf.database=redis

etcd_test: deps etcd_build unlink_brotli etcd_integration_test

etcd_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_ETCD_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/etcd.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/etcd.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/etcd.walgVersion=$(WALG_VERSION)")

etcd_install: etcd_build
	mv $(MAIN_ETCD_PATH)/wal-g $(GOBIN)/wal-g

etcd_clean:
	(cd $(MAIN_ETCD_PATH) && go clean)
	./cleanup.sh

# refactor
etcd_integration_test: load_docker_common
	docker compose build etcd_tests
	docker compose up --exit-code-from etcd_tests etcd_tests

gp_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_GP_PATH) && go build $(if $(ENABLE_RACE_DETECTION),-race) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -gcflags "$(BUILD_GCFLAGS)" -ldflags "$(BUILD_LDFLAGS) -X github.com/wal-g/wal-g/cmd/gp.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/gp.gitRevision=$(GIT_REVISION) -X github.com/wal-g/wal-g/cmd/gp.walgVersion=$(WALG_VERSION)")

gp_clean:
	(cd $(MAIN_GP_PATH) && go clean)
	./cleanup.sh

gp_install: gp_build
	mv $(MAIN_GP_PATH)/wal-g $(GOBIN)/wal-g

gp_test: deps gp_build unlink_brotli gp_integration_test

gp_integration_test: load_docker_common
	docker compose build gp
	docker compose build gp_tests
	docker compose up --exit-code-from gp_tests gp_tests

cloudberry_build: gp_build

cloudberry_clean: gp_clean

cloudberry_install: gp_install

cloudberry_test: deps cloudberry_build unlink_brotli cloudberry_integration_test

cloudberry_integration_test: load_docker_common
	docker compose build cloudberry
	docker compose build cloudberry_tests
	docker compose up s3 cloudberry_tests --force-recreate --exit-code-from cloudberry_tests

st_test: deps pg_build unlink_brotli st_integration_test

st_integration_test: load_docker_common
	docker compose build st_tests
	docker compose up --exit-code-from st_tests st_tests

unittest:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/...
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./pkg/...
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./utility/...

coverage:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go test -v $(TEST_MODIFIER) -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	go fmt ./...
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint:
	golangci-lint run --allow-parallel-runners ./...

docker_lint:
	docker build -t wal-g/lint --build-arg TAG=$(GOLANGCI_LINT_VERSION) - < docker/lint/Dockerfile
	docker run --rm -v `pwd`:/app \
		-v wal-g_lint_cache:/cache -e GOLANGCI_LINT_CACHE=/cache/lint \
		-e GOCACHE=/cache/go -e GOMODCACHE=/cache/gomod \
		wal-g/lint golangci-lint run -v

deps: go_deps link_external_deps

go_deps:
	if [ ! -f submodules/brotli/CMakeLists.txt ]; then git submodule update --init; fi
	cp CMakeLists-brotli.txt submodules/brotli/CMakeLists.txt
	go mod vendor
ifdef USE_LZO
	sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' vendor/github.com/cyberdelia/lzo/lzo.go
endif

link_external_deps: link_brotli link_libsodium

unlink_external_deps: unlink_brotli unlink_libsodium

install:
	@echo "Nothing to be done. Use pg_install/mysql_install/mongo_install/fdb_install/gp_install/etcd_install... instead."

link_brotli:
	@if [ -n "${USE_BROTLI}" ]; then ./link_brotli.sh; fi
	@if [ -z "${USE_BROTLI}" ]; then echo "info: USE_BROTLI is not set, skipping 'link_brotli' task"; fi

link_libsodium:
	@if [ ! -z "${USE_LIBSODIUM}" ]; then\
		./link_libsodium.sh;\
	fi

unlink_brotli:
	rm -rf vendor/github.com/google/brotli/*
	if [ -n "${USE_BROTLI}" ] ; then mv tmp/brotli/* vendor/github.com/google/brotli/; fi
	rm -rf tmp/brotli

unlink_libsodium:
	rm -rf tmp/libsodium

build_client:
	cd cmd/daemonclient && \
	go build -o ../../bin/walg-daemon-client $(if $(ENABLE_RACE_DETECTION),-race) -gcflags "$(BUILD_GCFLAGS)" -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X main.gitRevision=$(GIT_REVISION) -X main.version=$(WALG_VERSION)"

.PHONY: mocks
# put the files with interfaces you'd like to mock in prerequisites
# wildcards are allowed
mocks: $(FILE_TO_MOCKS)
	@echo "Generating mocks..."
	@rm -rf $(MOCKS_DESTINATION)
	@for file in $^; do mockgen -source=$$file -destination=$(MOCKS_DESTINATION)/$$(basename $$file); done
