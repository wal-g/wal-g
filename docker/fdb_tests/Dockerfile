FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY Makefile Makefile

ARG build_tags
ARG use_brotli

RUN [ -z $use_brotli ] || sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go
RUN cd main/fdb && \
    go build -mod vendor -tags "$build_tags" -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"
    
RUN make fdb_build

FROM foundationdb/foundationdb:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/fdb/wal-g /usr/bin

COPY docker/fdb_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
