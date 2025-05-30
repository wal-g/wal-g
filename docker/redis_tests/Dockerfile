FROM wal-g/golang:latest AS build

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

ENV USE_BROTLI=1
RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    cd main/redis && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/redis:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/main/redis/wal-g /usr/bin

COPY docker/redis_tests/scripts/ /tmp
COPY redis_cli.sh /usr/bin/redis_cli.sh

CMD /tmp/run_integration_tests.sh
