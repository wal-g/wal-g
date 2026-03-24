FROM wal-g/golang:latest AS build
ENV GOEXPERIMENT=jsonv2
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

RUN cd main/mysql && \
    go build -mod vendor -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/mariadb:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

RUN mkdir /root/testtools

COPY docker/mariadb_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
