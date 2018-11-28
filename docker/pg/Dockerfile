FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/

RUN cd cmd/wal-g && \
    go build -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/ubuntu:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/cmd/wal-g/wal-g /usr/bin

COPY docker/pg/scripts/ /tmp

CMD su postgres -c "/tmp/run_integration_tests.sh"
