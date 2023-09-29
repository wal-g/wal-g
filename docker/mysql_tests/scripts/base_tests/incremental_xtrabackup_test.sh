#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlincrementalxtrabackupbucket
export WALG_DELTA_MAX_STEPS=5
export WALG_DELTA_ORIGIN=LATEST

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start


# add data & create FULL backup:
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

# add data & create Incremental backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
sleep 1
wal-g backup-push

# add data & create second backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
wal-g backup-push

# this data will be lost
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"


# debug output:
wal-g st cat "${FIRST_BACKUP}/binlog_sentinel_005.json"
# debug output:
wal-g backup-list
wal-g st cat "${FIRST_BACKUP}/binlog_sentinel_005.json"

# restore full backup
mysql_kill_and_clean_data
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-fetch $FIRST_BACKUP

mysqldump sbtest > /tmp/dump_after_restore
grep -w 'testpitr01' /tmp/dump_after_restore
!grep -w 'testpitr02' /tmp/dump_after_restore
!grep -w 'testpitr03' /tmp/dump_after_restore
! grep -w 'testpitr04' /tmp/dump_after_restore


# restore all incremental backups
mysql_kill_and_clean_data
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
wal-g backup-fetch LATEST

mysqldump sbtest > /tmp/dump_after_restore
grep -w 'testpitr01' /tmp/dump_after_restore
grep -w 'testpitr02' /tmp/dump_after_restore
grep -w 'testpitr03' /tmp/dump_after_restore
! grep -w 'testpitr04' /tmp/dump_after_restore
