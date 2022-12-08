FROM wal-g/golang:latest as build

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
COPY Makefile Makefile
ENV USE_BROTLI 1

RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
    cd main/gp && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

RUN make gp_build

FROM wal-g/gp:latest

USER root

COPY docker/pg/PGP_KEY /tmp/PGP_KEY
COPY docker/pg/gpg.conf /home/gpadmin/.gnupg/gpg.conf
COPY docker/pg/gpg-agent.conf /home/gpadmin/.gnupg/gpg-agent.conf

COPY --from=build /go/src/github.com/wal-g/wal-g/main/gp/wal-g /usr/bin/

COPY docker/gp_tests/scripts/ /tmp
COPY docker/pg_tests/scripts/scripts/ /tmp/pg_scripts

RUN chmod 777 /tmp/configs/

RUN mkdir -p /var/log/
RUN chmod 777 /var/log/

RUN /bin/bash -c 'sudo chmod +x -R /tmp/tests; chmod +x /tmp/run_integration_tests.sh'

CMD su gpadmin bash -c '/tmp/run_integration_tests.sh'
