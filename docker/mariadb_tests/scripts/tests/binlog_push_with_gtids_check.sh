#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mariadb_binlog_push_with_gtids_check
export WALG_MYSQL_BINLOG_DST=/tmp/binlogs
export WALG_MYSQL_CHECK_GTIDS=True

mysql_install_db > /dev/null
service mysql start

sysbench --table-size=10 prepare
sysbench --time=3 run
mysql -e "FLUSH LOGS"

export WALG_MYSQL_CHECK_GTIDS=True
if wal-g binlog-push; then
  echo "WALG_MYSQL_CHECK_GTIDS is broken for MariaDB. It shouldn't allow users to misconfigure wal-g"
  exit 1
fi