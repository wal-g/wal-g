FROM wal-g/golang:latest AS build
WORKDIR /go/src/github.com/wal-g/wal-g

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY Makefile Makefile

# Any database binary exposes the storage tools ('wal-g st ...'); MySQL needs no extra C libraries to build.
RUN make mysql_build

FROM wal-g/ubuntu:22.04

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests rclone && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

COPY docker/rclone_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
