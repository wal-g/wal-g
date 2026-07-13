#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-bucket
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
mysql -e "FLUSH BINARY LOGS"

# binlog.000003 - empty
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push
sleep 1

# binlog.000004 from_binlog_01/02 are in the same binlog file with backup_and_binlog_01/02. binlog-server must skip backup_and_binlog_01 GTIDs and apply from_binlog_01/02
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('backup_and_binlog_02', NOW())"
wal-g backup-push
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
mysql -e "FLUSH BINARY LOGS"

# binlog.000005
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_03', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_04', NOW())"
mysql -e "FLUSH BINARY LOGS"

# binlog.000006
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_05', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_01', NOW())"
mysql -e "FLUSH BINARY LOGS"

# binlog.000007
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_02', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_03', NOW())"
mysql -e "FLUSH BINARY LOGS"

# binlog.000008
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_04', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_gtid_skip.log

WALG_LOG_LEVEL="DEVEL" wal-g binlog-server --since LATEST --until "$DT1" 2>&1 | tee $BINLOG_SERVER_LOG &
walg_pid=$!

sleep 3
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

wait $walg_pid

mysqldump sbtest > /tmp/dump_after_pitr_gtid_skip

# rows from backup 
grep -w 'backup_and_binlog_01' /tmp/dump_after_pitr_gtid_skip
grep -w 'backup_and_binlog_02' /tmp/dump_after_pitr_gtid_skip
# rows from post-backup binlogs before pitr time 
grep -w 'from_binlog_01' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_02' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_03' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_04' /tmp/dump_after_pitr_gtid_skip
grep -w 'from_binlog_05' /tmp/dump_after_pitr_gtid_skip
# rows after pitr time must be absent
! grep -w 'after_pitr_01' /tmp/dump_after_pitr_gtid_skip
! grep -w 'after_pitr_02' /tmp/dump_after_pitr_gtid_skip
! grep -w 'after_pitr_03' /tmp/dump_after_pitr_gtid_skip
! grep -w 'after_pitr_04' /tmp/dump_after_pitr_gtid_skip

#  expected to stream from mysql-bin.000003 to mysql-bin.000007
! grep -w 'Streaming mysql-bin.000002 to replica' $BINLOG_SERVER_LOG 
! grep -w 'Streaming mysql-bin.000008 to replica' $BINLOG_SERVER_LOG 

# assert that binlog-server skipped the duplicate GTIDs from the backup
SKIP_COUNT=$(grep -c "Skipping already-applied transaction" $BINLOG_SERVER_LOG || true)
if [ "$SKIP_COUNT" -ne 2 ]; then
    echo "ERROR: expected exactly two 'Skipping already-applied transaction' log line, got $SKIP_COUNT"
    cat $BINLOG_SERVER_LOG
    exit 1
fi
