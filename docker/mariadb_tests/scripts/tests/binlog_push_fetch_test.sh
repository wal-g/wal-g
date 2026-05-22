#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mariadb_binlog_push_fetch
export WALG_MYSQL_BINLOG_DST=/tmp/binlogs

mariadb_installdb
service mariadb start

# drop all before-dump binlogs, so SHOW BINARY LOGS will show all binlogs we need to fetch
current_binlog=$(mysql -e "SHOW BINARY LOGS" | tail -n 1 | awk '{print $1}')
mysql -e "PURGE BINARY LOGS TO '$current_binlog'";

wal-g backup-push
mysql -e "FLUSH LOGS"

sysbench --table-size=10 prepare
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push

sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push

# last binlog was not archived
current_binlog=$(mysql -e "SHOW BINARY LOGS" | tail -n 1 | awk '{print $1}')
mysql -N -e 'show binary logs' | awk '{print $1}' | grep -v "$current_binlog" > /tmp/proper_order

rm -rf /tmp/binlogs
mkdir /tmp/binlogs

# workaround for fetch, as default --until value doesn't include microseconds,
# so it may be less than binlog ctime's
sleep 2

wal-g binlog-fetch --since LATEST
diff -u /tmp/proper_order /tmp/binlogs/binlogs_order
while read -r binlog; do
    test -f /tmp/binlogs/"$binlog"
    ls -lah "$MYSQLDATA"/"$binlog" /tmp/binlogs/"$binlog"
    if ! cmp "$MYSQLDATA"/"$binlog" /tmp/binlogs/"$binlog"; then
        mysqlbinlog -v "$MYSQLDATA"/"$binlog" > /tmp/proper.sql
        mysqlbinlog -v /tmp/binlogs/"$binlog" > /tmp/fetched.sql
        diff -u /tmp/proper.sql /tmp/fetched.sql
    fi
done < /tmp/binlogs/binlogs_order
