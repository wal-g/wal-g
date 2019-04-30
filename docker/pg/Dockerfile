FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY utility/ utility/

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-dev && \
    cd cmd/wal-g && \
    go build -tags lzo -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/ubuntu:latest

ENV PGDATA /var/lib/postgresql/10/main

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    postgresql-10 \
    postgresql-contrib

RUN rm -rf $PGDATA