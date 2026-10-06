FROM wal-g/ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG MYSQL_SERIES=84

ENV MYSQLDATA=/var/lib/mysql

# Install MySQL and the matching XtraBackup from Percona repositories.
RUN apt-get update && \
    apt-get install --yes \
        bsdmainutils \
        ca-certificates \
        curl \
        gnupg2 \
        jq \
        lsb-release \
        s3cmd && \
    curl -fsSLO https://repo.percona.com/apt/percona-release_latest.generic_all.deb && \
    apt-get install --yes ./percona-release_latest.generic_all.deb && \
    case "$MYSQL_SERIES" in \
        57) server_repo=ps-57; backup_repo=pxb-24; server_suffix=-5.7; backup_series=24 ;; \
        80) server_repo=ps-80; backup_repo=pxb-80; server_suffix=; backup_series=80 ;; \
        84|97) server_repo=ps-${MYSQL_SERIES}-lts; backup_repo=pxb-${MYSQL_SERIES}-lts; \
            server_suffix=; backup_series=$MYSQL_SERIES ;; \
        *) echo "Unsupported MySQL series: $MYSQL_SERIES" >&2; exit 1 ;; \
    esac && \
    percona-release enable-only "$server_repo" release --scheme https && \
    percona-release enable "$backup_repo" release --scheme https && \
    apt-get update && \
    PERCONA_TELEMETRY_DISABLE=1 apt-get install --yes \
        percona-server-client${server_suffix} \
        percona-server-server${server_suffix} \
        percona-xtrabackup-${backup_series} \
        sysbench && \
    command -v mysql && \
    command -v mysqladmin && \
    command -v mysqldump && \
    command -v sysbench && \
    command -v xtrabackup && \
    rm -f percona-release_latest.generic_all.deb && \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf "$MYSQLDATA"

COPY docker/mysql/client.cnf /root/.my.cnf
COPY docker/mysql/client.cnf /etc/mysql/debian.cnf
COPY docker/mysql/init.sql /etc/mysql/init.sql
COPY docker/mysql/export_common.sh /usr/local/export_common.sh
COPY docker/mysql/export_test_funcs.sh /usr/local/export_test_funcs.sh
COPY docker/common/s3cfg /root/.s3cfg

# Append WAL-G test settings to the configuration installed by Percona Server.
COPY docker/mysql/my*.cnf /tmp/mysql-config/
RUN mysql_config="/tmp/mysql-config/my${MYSQL_SERIES}.cnf" && \
    if [ ! -f "$mysql_config" ]; then mysql_config=/tmp/mysql-config/my.cnf; fi && \
    cat "$mysql_config" >> /etc/mysql/my.cnf && rm -r /tmp/mysql-config

# MySQL 5.7 cannot read the heartbeat V2 events sent by binlog-server.
RUN if [ "$MYSQL_SERIES" = 57 ]; then \
        echo 'export WALG_MYSQL_BINLOG_SERVER_DISABLE_HEARTBEAT=true' >> /usr/local/export_common.sh; \
    fi
