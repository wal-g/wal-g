FROM wal-g/golang:latest AS build
WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests git

COPY .git .git
COPY .gitmodules .gitmodules
COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY test/ test/
COPY testtools/ testtools/
COPY link_brotli.sh link_brotli.sh
COPY CMakeLists-brotli.txt CMakeLists-brotli.txt
COPY Makefile Makefile

RUN go get -v -t ./... && USE_BROTLI=1 make ENABLE_RACE_DETECTION=1 deps redis_build

FROM wal-g/redis:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/main/redis/wal-g /usr/bin

COPY docker/redis_tests/scripts/ /tmp
COPY redis_cli.sh /usr/bin/redis_cli.sh

CMD /tmp/run_integration_tests.sh
