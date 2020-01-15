# vim:set ft=dockerfile:
FROM mongodb-backup-base

ARG MONGO_PACKAGE=mongodb-org
ARG MONGO_REPO=repo.mongodb.org
ARG WALG_REPO=${GOPATH}/src/github.com/wal-g/wal-g

ENV MONGO_PACKAGE=${MONGO_PACKAGE} MONGO_REPO=${MONGO_REPO}
ARG MONGO_MAJOR=3.6
ARG MONGO_VERSION=3.6.3
ENV TMP_DIR /var/tmp/wal-g

RUN echo "deb http://$MONGO_REPO/apt/ubuntu xenial/${MONGO_PACKAGE%-unstable}/$MONGO_MAJOR multiverse" | tee "/etc/apt/sources.list.d/${MONGO_PACKAGE%-unstable}.list"
RUN apt-get update -qq && \
    apt-get install -y apt-transport-https tzdata && \
    mkdir -p /etc/apt/sources.list.d && \
    apt-get update -qq && \
    apt-get install --allow-unauthenticated -y \
        ${MONGO_PACKAGE}-server=$MONGO_VERSION \
        ${MONGO_PACKAGE}-shell=$MONGO_VERSION \
        ${MONGO_PACKAGE}-tools=$MONGO_VERSION \
        pv && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/lib/mongodb/* && \
    apt-get clean

RUN mkdir -p /home/mongodb /var/log/wal-g/ ${TMP_DIR} && \
    chown mongodb:mongodb /home/mongodb /var/log/wal-g/ && \
    ln --force -s /config/supervisor/conf.d/mongodb.conf /etc/supervisor/conf.d/mongodb.conf && \
    mkdir -p ${WALG_REPO}

COPY wal-g ${WALG_REPO}/
WORKDIR ${WALG_REPO}
RUN make install && \
    git submodule update --init && \
    sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' vendor/github.com/cyberdelia/lzo/lzo.go && \
    ./link_brotli.sh && \
    ./link_libsodium.sh && \
    make mongo_build && \
    cp main/mongo/wal-g /usr/bin/wal-g