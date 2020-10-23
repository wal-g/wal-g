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

COPY docker/mysql/client.cnf /root/.my.cnf
COPY docker/mysql/client.cnf /etc/mysql/debian.cnf
COPY docker/mysql/init.sql /etc/mysql/init.sql
COPY docker/mysql/export_common.sh /usr/local/export_common.sh
COPY docker/mysql/export_test_funcs.sh /usr/local/export_test_funcs.sh

# append
COPY docker/mysql/my.cnf /tmp/my.cnf
RUN cat /tmp/my.cnf >> /etc/mysql/my.cnf; rm /tmp/my.cnf
