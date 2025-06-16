FROM bitnami/etcd:3.5.21
# in 3.6 which is latest "etcdctl snapshot restore" -> "etcdutl snapshot restore" 

# FROM wal-g/ubuntu:latest


# RUN apt-get update && \
#     apt-get install --yes --no-install-recommends --no-install-suggests \
#     etcd

COPY docker/etcd/export_common.sh /usr/local/export_common.sh