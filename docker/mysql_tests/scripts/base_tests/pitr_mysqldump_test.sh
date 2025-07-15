#!/bin/sh
set -e -x

. /usr/local/export_common.sh

echo "SKIPED"; exit 0

export WALE_S3_PREFIX=s3://mysqlpitrmysqldumpbucket
export WALG_STREAM_CREATE_COMMAND="mysqldump --all-databases --single-transaction"
export WALG_STREAM_RESTORE_COMMAND="mysql"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND=

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

# first full backup
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
sleep 1

# second full backup
wal-g backup-push
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
sleep 1

DT1=$(date3339)

sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push


# pitr restore after LATEST backup
mysql_kill_and_clean_data
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-fetch LATEST

wal-g binlog-replay --since LATEST --until "$DT1"
mysqldump sbtest > /tmp/dump_after_pitr
grep -w 'testpitr01' /tmp/dump_after_pitr
grep -w 'testpitr02' /tmp/dump_after_pitr
grep -w 'testpitr03' /tmp/dump_after_pitr
! grep -w 'testpitr04' /tmp/dump_after_pitr


# pitr restore across full backup
mysql_kill_and_clean_data
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-fetch $FIRST_BACKUP

wal-g binlog-replay --since $FIRST_BACKUP --until "$DT1"
mysqldump sbtest > /tmp/dump_after_pitr
grep -w 'testpitr01' /tmp/dump_after_pitr
grep -w 'testpitr02' /tmp/dump_after_pitr
grep -w 'testpitr03' /tmp/dump_after_pitr
! grep -w 'testpitr04' /tmp/dump_after_pitr
