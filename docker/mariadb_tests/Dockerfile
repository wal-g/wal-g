FROM wal-g/golang:latest AS build
WORKDIR /go/src/github.com/wal-g/wal-g

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY Makefile Makefile

RUN make ENABLE_RACE_DETECTION=1 mysql_build

FROM wal-g/mariadb:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

RUN mkdir /root/testtools

COPY docker/mariadb_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
