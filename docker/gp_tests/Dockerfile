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


RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    cd main/gp && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

RUN make gp_build

RUN cd main/pg && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

RUN make pg_build

FROM wal-g/gp:latest

USER root

COPY --from=build /go/src/github.com/wal-g/wal-g/main/gp/wal-g /usr/bin/wal-g-gp
COPY --from=build /go/src/github.com/wal-g/wal-g/main/pg/wal-g /usr/bin

COPY docker/gp_tests/scripts/ /tmp
RUN /bin/bash -c 'sudo chmod +x -R /tmp/tests; chmod +x /tmp/run_integration_tests.sh'

CMD su gpadmin bash -c '/tmp/run_integration_tests.sh'
