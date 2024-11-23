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

RUN wget https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-x86_64.tar.gz && \
    tar xf cmake-3.31.0-linux-x86_64.tar.gz -C /usr --strip-components=1

COPY submodules/ tmp/

RUN cd tmp/brotli && \
    mkdir out && cd out && \
    cmake .. && \
    make && make install

RUN sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go && \
        cd main/etcd && \
        go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.buildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

RUN make etcd_build

FROM wal-g/etcd:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/etcd/wal-g /usr/bin

COPY docker/etcd_tests/scripts/ /tmp
USER root

CMD /tmp/run_integration_tests.sh