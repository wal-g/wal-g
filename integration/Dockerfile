# vim:set ft=dockerfile:
FROM dbaas-mongodb-backup-base

ENV TMP_DIR /var/tmp/wal-g

#ARG repository="deb http://dist.yandex.ru/mail-trusty stable/all/"
## ARG version=\*
ARG version=3.6.3+yandex5

RUN apt-get update -qq && \
    apt-get install -y apt-transport-https tzdata && \
    mkdir -p /etc/apt/sources.list.d && \
    apt-get update -qq && \
    apt-get install --allow-unauthenticated -y \
        mongodb-org-server=$version \
        mongodb-org-tools=$version \
        mongodb-org-shell=$version \
        pv && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/lib/mongodb/* && \
    apt-get clean

RUN mkdir -p /home/mongodb /var/log/wal-g/ ${TMP_DIR} && \
    chown mongodb:mongodb /home/mongodb /var/log/wal-g/ && \
    ln --force -s /config/supervisor/conf.d/mongodb.conf /etc/supervisor/conf.d/mongodb.conf

COPY wal-g /usr/bin/wal-g
