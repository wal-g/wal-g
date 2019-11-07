FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests

COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    cd main/mysql && \
    go build -tags brotli -race -o wal-g -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

COPY docker/mysql/gtid_set.go /tmp/testtools/gtid_set.go

RUN go get github.com/jmoiron/sqlx github.com/go-sql-driver/mysql && go build -o /bin/setter /tmp/testtools/gtid_set.go

FROM wal-g/mysql:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin
COPY --from=build /bin/setter /usr/bin

RUN mkdir /root/testtools
COPY docker/mysql/test_apply.sh /root/testtools/test_apply.sh

COPY docker/mysql_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
