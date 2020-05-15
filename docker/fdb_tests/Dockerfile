FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY Makefile Makefile

RUN make fdb_build

FROM foundationdb/foundationdb:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/fdb/wal-g /usr/bin

COPY docker/fdb_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
