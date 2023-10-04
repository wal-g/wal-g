#!/bin/sh
set -e -x

# In this test we will cover `wal-g delete` section of README.md

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
FIRST_FULL_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

# add data & create Incremental backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr02', NOW())"
sleep 1
wal-g backup-push
FIRST_INC_BACKUP=$(wal-g backup-list | grep -v "$FIRST_FULL_BACKUP" | awk 'NR==2{print $1}')

sleep 1
DT_BEFORE_SECOND_FULL=$(date +%Y-%M-%dT%R:%S)
sleep 1

# add data & create second full backup
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr03', NOW())"
wal-g backup-push --full
SECOND_FULL_BACKUP=$(wal-g backup-list | grep -v "$FIRST_FULL_BACKUP" | grep -v "$FIRST_INC_BACKUP" | awk 'NR==2{print $1}')
sleep 1

# this data will be lost
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr04', NOW())"
wal-g backup-push
SECOND_INC_BACKUP=$(wal-g backup-list | grep -v "$FIRST_FULL_BACKUP" | grep -v "$FIRST_INC_BACKUP" | grep -v "$SECOND_FULL_BACKUP" | awk 'NR==2{print $1}')

# debug output:
wal-g backup-list
wal-g st ls basebackups_005/

# backup our S3:
mkdir -p /tmp/s3 | true
s3cmd sync $WALE_S3_PREFIX/basebackups_005/ /tmp/s3/

cat <<EOF
##########
# wal-g delete everything
# all backups will be deleted (if there are no permanent backups)
##########
EOF
s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
! wal-g delete everything --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete all backups. Diff backups are not protected"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
! grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
! grep -w "$SECOND_INC_BACKUP" /tmp/backup_list


cat <<EOF
##########
# wal-g delete everything FORCE
# all backups will be deleted (if there are no permanent backups)
##########
EOF
s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete everything FORCE --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: all backups deleted"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
! grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
! grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



cat <<EOF
##########
# wal-g delete retain 5
# will fail if 5th is delta
##########
EOF
s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
! wal-g delete retain 3 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete nothing when 3-rd backup is diff-backup"
grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

wal-g delete retain 2 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should left only 2 last backups"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



cat <<EOF
##########
# wal-g delete retain FULL 5
# will keep 5 full backups and all deltas of them
##########
EOF
s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete retain FULL 1 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete retain FULL 2 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should leave 2 FULL backups and its increments"
grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



cat <<EOF
##########
# wal-g delete retain FIND_FULL 5
# will find necessary full for 5th and keep everything after it
##########
EOF
s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete retain FIND_FULL 1 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete retain FIND_FULL 2 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
! wal-g delete retain FIND_FULL 3 --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: nothing should be deleted"
grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



#
# It is broken for MySQL and Pg... probably it never worked
#
#cat <<EOF
###########
## wal-g delete retain --after 2019-12-12T12:12:12
## keep 5 most recent backups and backups made after 2019-12-12 12:12:12
###########
#EOF
#s3cmd sync /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
#wal-g delete retain 5 --after $DT_BEFORE_SECOND_FULL --confirm
#wal-g backup-list > /tmp/backup_list
#echo "# Expected: should delete old FULL-backup + all its increments"
#! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
#! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
#grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
#grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



cat <<EOF
##########
# wal-g delete before base_000010000123123123
# will fail if `base_000010000123123123` is delta
##########
EOF
s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
! wal-g delete before $FIRST_INC_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: nothing should be deleted"
grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete before $SECOND_FULL_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list


cat <<EOF
##########
# wal-g delete before base_000010000123123123
# will keep everything after base of base_000010000123123123
##########
EOF
s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
! wal-g delete before FIND_FULL $FIRST_INC_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: nothing should be deleted"
grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete before FIND_FULL $SECOND_FULL_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete before FIND_FULL $SECOND_INC_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list



cat <<EOF
##########
# wal-g delete target base_0000000100000000000000C9
# delete the base backup and all dependant delta backups
##########
EOF
s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete target $FIRST_INC_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete target $FIRST_FULL_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list




cat <<EOF
##########
# wal-g delete target FIND_FULL base_0000000100000000000000C9
# delete delta backup and all delta backups with the same base backup
##########
EOF
s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete target FIND_FULL $FIRST_INC_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list

s3cmd sync -q /tmp/s3/ $WALE_S3_PREFIX/basebackups_005/
wal-g delete target FIND_FULL $FIRST_FULL_BACKUP --confirm
wal-g backup-list > /tmp/backup_list
echo "# Expected: should delete old FULL-backup + all its increments"
! grep -w "$FIRST_FULL_BACKUP" /tmp/backup_list
! grep -w "$FIRST_INC_BACKUP" /tmp/backup_list
grep -w "$SECOND_FULL_BACKUP" /tmp/backup_list
grep -w "$SECOND_INC_BACKUP" /tmp/backup_list