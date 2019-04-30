FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-dev

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

RUN cd main/mysql && \
    go build -o wal-g -tags lzo -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/mysql:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

COPY docker/mysql_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh