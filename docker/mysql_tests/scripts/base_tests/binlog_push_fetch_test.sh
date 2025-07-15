#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlbinlogpushfetchbucket
export WALG_MYSQL_BINLOG_DST=/tmp/binlogs
export WALG_MYSQL_CHECK_GTIDS=True

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

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


echo "Get GTIDs, and write HUGE GTID to cache, so no binlogs should be uploaded anymore"
sysbench --time=1 run && mysql -e "FLUSH LOGS"
sysbench --time=1 run && mysql -e "FLUSH LOGS"
sysbench --time=1 run && mysql -e "FLUSH LOGS"
current_uuid=$(mysql -Nse "SELECT @@server_uuid" | awk '{print $1}')
current_sentinel=$(s3cmd get "${WALE_S3_PREFIX}/binlog_sentinel_005.json" - )
echo "{\"GtidArchived\":\"${current_uuid}:1-999999\"}" | s3cmd put - "${WALE_S3_PREFIX}/binlog_sentinel_005.json"
rm -f "$HOME/.walg_mysql_binlogs_cache"
binlogs_cnt1=$(s3cmd ls "${WALE_S3_PREFIX}/binlog_005/" | wc -l )
wal-g binlog-push
binlogs_cnt2=$(s3cmd ls "${WALE_S3_PREFIX}/binlog_005/" | wc -l )

if [ "$binlogs_cnt1" -ne "$binlogs_cnt2" ]; then
  echo "Some binlogs has been uploaded... however it shouldn't be: ${binlogs_cnt1} != ${binlogs_cnt2}"
  exit 1
fi

echo "Revert GTIDs in cache, so all binlogs should be uploaded"
echo "${current_sentinel}}" | s3cmd put - "${WALE_S3_PREFIX}/binlog_sentinel_005.json"
rm -f "$HOME/.walg_mysql_binlogs_cache"
wal-g binlog-push
binlogs_cnt3=$(s3cmd ls "${WALE_S3_PREFIX}/binlog_005/" | wc -l )

if [ "$binlogs_cnt2" -eq "$binlogs_cnt3" ]; then
  echo "Some binlogs haven't been uploaded... however it should"
  exit 1
fi
