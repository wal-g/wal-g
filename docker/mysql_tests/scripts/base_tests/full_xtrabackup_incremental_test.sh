#!/bin/sh
set -e -x

. /usr/local/export_common.sh

#
# In this test we check that wal-g can create incremental backups & restore them
#

export WALE_S3_PREFIX=s3://mysqlincrementalxtrabackupbucket

# Required configuration:
export WALG_DELTA_MAX_STEPS=5
export WALG_DELTA_ORIGIN=LATEST

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

# add data & create FULL backup:
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"
wal-g xtrabackup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

# add data & create Incremental backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
sleep 1
wal-g xtrabackup-push --full=FALSE
SECOND_BACKUP=$(wal-g backup-list | grep -v "$FIRST_BACKUP" | awk 'NR==2{print $1}')

# add data & create second backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
wal-g xtrabackup-push --full=false
LATEST_BACKUP=$(wal-g backup-list | grep -v "$FIRST_BACKUP" | grep -v "$SECOND_BACKUP" | awk 'NR==2{print $1}')

# this data will be lost
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"


# debug output:
wal-g backup-list
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json"

# restore full backup
mysql_kill_and_clean_data
wal-g backup-fetch $FIRST_BACKUP
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)

mysqldump sbtest > /tmp/dump_after_restore
grep -w 'testpitr01' /tmp/dump_after_restore
! grep -w 'testpitr02' /tmp/dump_after_restore
! grep -w 'testpitr03' /tmp/dump_after_restore
! grep -w 'testpitr04' /tmp/dump_after_restore


# restore all incremental backups
mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)

mysqldump sbtest > /tmp/dump_after_restore
grep -w 'testpitr01' /tmp/dump_after_restore
grep -w 'testpitr02' /tmp/dump_after_restore
grep -w 'testpitr03' /tmp/dump_after_restore
! grep -w 'testpitr04' /tmp/dump_after_restore
