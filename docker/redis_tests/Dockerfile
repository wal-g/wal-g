FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

RUN cd main/redis && \
    go build -o wal-g -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/redis:latest

COPY --from=build /go/src/github.com/wal-g/wal-g/main/redis/wal-g /usr/bin

COPY docker/redis_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
