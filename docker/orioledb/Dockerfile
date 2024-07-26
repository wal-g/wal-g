FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-dev

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' \
        vendor/github.com/cyberdelia/lzo/lzo.go
RUN --mount=type=cache,target=/gocache cd main/pg && GOCACHE=/gocache \
    go build -mod vendor -race -o wal-g -tags "brotli lzo" -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"
RUN --mount=type=cache,target=/gocache cd cmd/daemonclient && GOCACHE=/gocache \
    go build -mod vendor -o walg-daemon-client -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

RUN --mount=type=cache,target=/gocache cd pkg/storages/swift && GOCACHE=/gocache go test -v -c
RUN --mount=type=cache,target=/gocache cd pkg/storages/sh && GOCACHE=/gocache go test -v -c


FROM wal-g/ubuntu:20.04

ENV PGDATA /var/lib/postgresql/16/main

RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends --no-install-suggests \
		wget \
		curl

RUN mkdir -p /usr/src/postgresql/contrib/orioledb && chmod 777 /usr/src/postgresql/contrib/orioledb

WORKDIR /usr/src/postgresql/contrib/orioledb
RUN set -eux; \
	git init; \
	git remote add origin https://github.com/orioledb/orioledb.git; \
	git -c protocol.version=2 fetch --no-tags --prune --progress --no-recurse-submodules --depth=1 origin +466738fe9d9a0f225ee5e5c776182ad426ae2008:refs/remotes/origin/main; \
	git checkout --progress --force -B main refs/remotes/origin/main;

WORKDIR /
RUN set -eux; \
	\
	# don't forget to change PGTAG below
	PGTAG=$(grep "^16: " /usr/src/postgresql/contrib/orioledb/.pgtags | cut -d' ' -f2-) ; \
	\
	curl -o postgresql.tar.gz \
			--header "Accept: application/vnd.github.v3.raw" \
			--remote-name \
			--location https://github.com/orioledb/postgres/tarball/$PGTAG; \
	mkdir -p /usr/src/postgresql; \
	tar \
		--extract \
		--file postgresql.tar.gz \
		--directory /usr/src/postgresql \
		--strip-components 1 \
	; \
	rm postgresql.tar.gz;

WORKDIR /usr/src/postgresql
RUN set -eux; \
	apt-get update; \
	DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends \
		build-essential \
		llvm-dev clang \
		bison \
		daemontools \
		flex \
		libcurl4-openssl-dev \
		libicu-dev \
		liblz4-1 \
		liblz4-dev \
		libz-dev \
		libssl-dev \
		libreadline-dev \
		libuuid1 \
		libzstd1 \
		libzstd-dev \
		make \
		pkg-config \
		uuid-dev \
		wget

RUN apt search zstd

WORKDIR /usr/src/postgresql
ENV PATH="$PATH:/usr/local/bin"
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"
RUN set -eux; \
	\
	# don't forget to change PGTAG above
	PGTAG=$(grep "^16: " /usr/src/postgresql/contrib/orioledb/.pgtags | cut -d' ' -f2-) ; \
	ORIOLEDB_VERSION=$(grep "^#define ORIOLEDB_VERSION" /usr/src/postgresql/contrib/orioledb/include/orioledb.h | cut -d'"' -f2) ; \
	ORIOLEDB_BUILDTIME=$(date -Iseconds) ; \
	\
	gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
	( CC=clang ./configure \
		--build="$gnuArch" \
		--enable-integer-datetimes \
		--enable-thread-safety \
		--disable-rpath \
		--with-uuid=e2fs \
		--with-gnu-ld \
		--with-pgport=5432 \
		--with-system-tzdata=/usr/share/zoneinfo \
		--prefix=/usr/local \
		--with-includes=/usr/local/include \
		--with-libraries=/usr/local/lib \
		--with-openssl \
		--with-icu \
		--with-llvm \
		--with-lz4 \
		--with-zstd \
		--with-extra-version=" ${ORIOLEDB_VERSION} PGTAG=${PGTAG} ubuntu:focal+clang build:${ORIOLEDB_BUILDTIME}" \
	|| cat config.log ); \
	echo "ORIOLEDB_PATCHSET_VERSION = `echo $PGTAG | cut -d'_' -f2`" >> src/Makefile.global; \
	make -j "$(nproc)"; \
	make -C contrib -j "$(nproc)"; \
	make -C contrib/orioledb -j "$(nproc)"; \
	make install; \
	make -C contrib install; \
	make -C contrib/orioledb install; \
	postgres --version

COPY docker/common/s3cfg /var/lib/postgresql/.s3cfg

COPY docker/pg/walg.json /tmp/walg.json

RUN rm -rf $PGDATA

# explicitly set user/group IDs
RUN set -eux; \
	groupadd -r postgres --gid=999; \
# https://salsa.debian.org/postgresql/postgresql-common/blob/997d842ee744687d99a2b2d95c1083a2615c79e8/debian/postgresql-common.postinst#L32-35
	useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres; \
# also create the postgres user's home directory with appropriate permissions
# see https://github.com/docker-library/postgres/issues/274
	mkdir -p /var/lib/postgresql; \
	chown -R postgres:postgres /var/lib/postgresql

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y netcat-openbsd && apt-get clean

COPY --from=build /go/src/github.com/wal-g/wal-g/pkg/storages/swift/swift.test /tmp/test_bins/
COPY --from=build /go/src/github.com/wal-g/wal-g/pkg/storages/sh/sh.test /tmp/test_bins/
COPY --from=build /go/src/github.com/wal-g/wal-g/main/pg/wal-g /usr/bin
COPY --from=build /go/src/github.com/wal-g/wal-g/cmd/daemonclient/walg-daemon-client /usr/bin

COPY docker/pg_tests/scripts/scripts/ /tmp/scripts/
COPY docker/pg_tests/scripts/configs/common_config.json /tmp/configs/
COPY docker/orioledb/scripts/ /tmp

RUN mkdir -p /tmp/logs/ && chmod 777 /tmp/configs/ && chmod 777 /tmp/logs/
RUN mkdir -p /var/run/postgresql && chmod 777 /var/run/postgresql
ENV PGHOST /var/run/postgresql
