FROM wal-g/ubuntu:latest

ENV CLICKHOUSE_BACKUP_VERSION "1.0.0-beta1"

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    gnupg

RUN echo "deb https://repo.yandex.ru/clickhouse/deb/stable main/" | tee /etc/apt/sources.list.d/clickhouse.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    wget \
    clickhouse-server \
    clickhouse-client

WORKDIR /usr/lib

RUN wget -O clickhouse-backup.tar.gz https://github.com/AlexAkulov/clickhouse-backup/releases/download/v${CLICKHOUSE_BACKUP_VERSION}/clickhouse-backup.tar.gz
RUN tar -zxvf clickhouse-backup.tar.gz

COPY docker/clickhouse/config.yml /usr/lib/clickhouse-backup/

ENV PATH="/usr/lib/clickhouse-backup:${PATH}"
