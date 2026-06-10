FROM wal-g/base

ARG ETCD_VERSION=v3.5.31
# stay on 3.5: in 3.6 "etcdctl snapshot restore" -> "etcdutl snapshot restore"

# official upstream binaries: etcd, etcdctl, etcdutl
RUN ARCH="$(dpkg --print-architecture)" && \
    wget -qO- "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ARCH}.tar.gz" \
      | tar xz --strip-components=1 -C /usr/local/bin \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcd" \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcdctl" \
          "etcd-${ETCD_VERSION}-linux-${ARCH}/etcdutl"

COPY docker/etcd/export_common.sh /usr/local/export_common.sh
