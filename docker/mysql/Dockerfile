FROM wal-g/ubuntu:latest

ENV MYSQLDATA /var/lib/mysql

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    curl \
    mysql-server \
    mysql-client \
    percona-xtrabackup


RUN curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | bash && apt -y install sysbench
RUN rm -rf $MYSQLDATA

COPY docker/mysql/init.sql /etc/mysql/init.sql
COPY docker/mysql/kill_mysql_and_cleanup_data.sh /usr/bin/kill_mysql_and_cleanup_data
COPY docker/mysql/export_common.sh /usr/local/export_common.sh

# append
COPY docker/mysql/my.cnf /root/my.cnf
RUN cat /root/my.cnf >> /etc/mysql/my.cnf