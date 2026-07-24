FROM wal-g/golang:latest AS build
WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests git

COPY .git .git
COPY .gitmodules .gitmodules
COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/
COPY test/ test/
COPY testtools/ testtools/
COPY link_brotli.sh link_brotli.sh
COPY CMakeLists-brotli.txt CMakeLists-brotli.txt
COPY Makefile Makefile

RUN wget https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-linux-x86_64.tar.gz && \
    tar xf cmake-3.31.0-linux-x86_64.tar.gz -C /usr --strip-components=1

RUN USE_BROTLI=1 make ENABLE_RACE_DETECTION=1 deps etcd_build

FROM wal-g/base

ARG ETCD_VERSION=v3.5.33
# stay on 3.5: in 3.6 "etcdctl snapshot restore" -> "etcdutl snapshot restore"

# official upstream binaries: etcd, etcdctl, etcdutl
RUN ARCH="$(dpkg --print-architecture)" && \
    curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ARCH}.tar.gz" \
      | tar xz --strip-components=1 -C /usr/local/bin \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcd" \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcdctl" \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcdutl"

COPY --from=build /go/src/github.com/wal-g/wal-g/main/etcd/wal-g /usr/bin
COPY docker/etcd_tests/export_common.sh /usr/local/export_common.sh
COPY docker/etcd_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh