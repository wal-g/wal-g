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

RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    cd main/clickhouse && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/clickhouse:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/clickhouse/wal-g /usr/bin

RUN mkdir /tmp/wal-g-test-data

COPY docker/clickhouse_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
