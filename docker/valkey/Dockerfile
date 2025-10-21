FROM wal-g/ubuntu:18.04

ENV WALG_VALKEY_DATA_FOLDER=/var/lib/valkey
ENV VALKEY_VERSION=9.0.0

RUN mkdir $WALG_VALKEY_DATA_FOLDER

RUN apt-get clean && \
    apt-get update && apt-get install wget build-essential -y && \
    wget https://github.com/valkey-io/valkey/archive/refs/tags/${VALKEY_VERSION}.tar.gz && \
    tar xzf "${VALKEY_VERSION}.tar.gz" && \
    cd "valkey-${VALKEY_VERSION}" && \
    make install

COPY docker/valkey/valkey.conf /etc/valkey/valkey.conf
