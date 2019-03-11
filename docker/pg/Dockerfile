FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-dev && \
    cd main/wal-g && \
    go build -tags lzo -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/ubuntu:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/main/wal-g/wal-g /usr/bin

COPY docker/pg/scripts/ /tmp

CMD su postgres -c "/tmp/run_integration_tests.sh"
