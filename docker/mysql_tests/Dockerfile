FROM wal-g/golang:latest AS build
WORKDIR /go/src/github.com/wal-g/wal-g

COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY Makefile Makefile
COPY submodules/ submodules/
COPY test/ test/
COPY testtools/ testtools/
COPY link_brotli.sh link_brotli.sh
COPY CMakeLists-brotli.txt CMakeLists-brotli.txt

RUN USE_BROTLI=1 make ENABLE_RACE_DETECTION=1 deps mysql_build

FROM wal-g/mysql:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

COPY docker/mysql_tests/scripts/ /tmp

ARG TEST_ARG=""
ENV TEST_ARG=${TEST_ARG}

CMD /tmp/run_integration_tests.sh ${TEST_ARG}
