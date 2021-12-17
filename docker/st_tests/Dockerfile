FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-dev

RUN ls

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

ARG build_tags
ARG use_brotli
ARG use_lzo
ARG walg_compression_method
ENV WALG_COMPRESSION_METHOD $walg_compression_method

RUN [ -z $use_brotli ] || sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go
RUN [ -z $use_lzo ] || sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' \
        vendor/github.com/cyberdelia/lzo/lzo.go
RUN cd main/pg && \
    go build -mod vendor -race -o wal-g -tags "$build_tags" -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/ubuntu:latest

RUN apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests brotli

COPY --from=build /go/src/github.com/wal-g/wal-g/main/pg/wal-g /usr/bin

COPY docker/st_tests/scripts/ /tmp

CMD /tmp/tests/storage_tool_tests.sh
