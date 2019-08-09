# vim:set ft=dockerfile:
FROM mongodb-backup-base

ENV TMP_DIR /var/tmp/wal-g

ARG MONGO_PACKAGE=mongodb-org
ARG MONGO_REPO=repo.mongodb.org
ENV MONGO_PACKAGE=${MONGO_PACKAGE} MONGO_REPO=${MONGO_REPO}

ENV MONGO_MAJOR 3.6
ENV MONGO_VERSION 3.6.3
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
    ln --force -s /config/supervisor/conf.d/mongodb.conf /etc/supervisor/conf.d/mongodb.conf

COPY wal-g /usr/bin/wal-g
