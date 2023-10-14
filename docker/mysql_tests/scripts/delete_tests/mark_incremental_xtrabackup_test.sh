#!/bin/sh
set -e -x

. /usr/local/export_common.sh

#
# In this test we check that wal-g can mark incremental backups as permanent and all parent backups will be marked as
# permanent as well
#

export WALE_S3_PREFIX=s3://mysqlmarkincrementalbucket

# Required configuration:
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
wal-g backup-push --full=FALSE
SECOND_BACKUP=$(wal-g backup-list | grep -v "$FIRST_BACKUP" | awk 'NR==2{print $1}')

# add data & create second backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
wal-g backup-push --full=false
LATEST_BACKUP=$(wal-g backup-list | grep -v "$FIRST_BACKUP" | grep -v "$SECOND_BACKUP" | awk 'NR==2{print $1}')


# debug output:
wal-g backup-list
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json"

echo "# mark incremental backup as permanent:"
wal-g backup-mark -b $SECOND_BACKUP
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"  | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "false"

echo "# mark incremental backup as permanent [2]:"
wal-g backup-mark -b $LATEST_BACKUP
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"  | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "true"

echo "# mark intermediate backup as impermanent: (expect error)"
! wal-g backup-mark -b $SECOND_BACKUP -i

echo "# mark intermediate backup as impermanent:"
wal-g backup-mark -b $LATEST_BACKUP -i
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"  | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "true"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "false"

echo "# mark all backup as impermanent (one by one):"
wal-g backup-mark -b $LATEST_BACKUP -i
wal-g backup-mark -b $SECOND_BACKUP -i
wal-g backup-mark -b $FIRST_BACKUP -i
wal-g st cat "basebackups_005/${FIRST_BACKUP}_backup_stop_sentinel.json"  | jq ".IsPermanent" | grep "false"
wal-g st cat "basebackups_005/${SECOND_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "false"
wal-g st cat "basebackups_005/${LATEST_BACKUP}_backup_stop_sentinel.json" | jq ".IsPermanent" | grep "false"