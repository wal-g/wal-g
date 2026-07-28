#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-until-lmt-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

# binlog.000002
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g backup-push

# binlog.000003
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_03', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_04', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

# binlog.000004
mysql -e "INSERT INTO sbtest.pitr VALUES('lmt_ignored_01', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_until_lmt.log

# DT1 is used as both PITR time (--until) and the binlog last-modified cutoff
# (--until-binlog-last-modified-time).  binlog.000002 and 000003 were pushed
# to S3 before DT1, so they are eligible.  binlog.000004 was pushed after
# DT1, so it must be filtered out by endBinlogTS even though lmt_ignored_01
# (inside it) is valid data before PITR time.
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server \
    --since LATEST \
    --until "$DT1" \
    --until-binlog-last-modified-time "$DT1" \
    2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!

sleep 3
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

wait $walg_pid

mysqldump sbtest > /tmp/dump_after_pitr_until_lmt

# rows from binlog.000002 and 000003 (pushed before LMT cutoff, before PITR time)
grep -w 'from_binlog_01' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_02' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_03' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_04' /tmp/dump_after_pitr_until_lmt

# lmt_ignored_01 is in binlog.000004 which was pushed to S3 after DT1 (LMT),
# so it must be absent even though the data is before PITR time
! grep -w 'lmt_ignored_01' /tmp/dump_after_pitr_until_lmt

# rows after pitr time must be absent
! grep -w 'after_pitr_01' /tmp/dump_after_pitr_until_lmt

# binlog-server must not stream binlog.000004 (pushed after LMT cutoff)
! grep -w 'Streaming mysql-bin.000004 to replica' $BINLOG_SERVER_LOG

# binlog-server must stream binlog.000002 and 000003 (pushed before LMT cutoff)
grep -w 'Streaming mysql-bin.000002 to replica' $BINLOG_SERVER_LOG
grep -w 'Streaming mysql-bin.000003 to replica' $BINLOG_SERVER_LOG
