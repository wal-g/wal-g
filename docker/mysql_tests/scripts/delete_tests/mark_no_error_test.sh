#!/bin/sh
set -e -x

. /usr/local/export_common.sh

#
# In this test we check that when `wal-g backup-mark` executed twice - we don't have error
#

export WALE_S3_PREFIX=s3://mysqlmarknoerrorbucket

# initialize mysql
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
# add data & create FULL backup:
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('testpitr01', NOW())"

# backup
wal-g backup-push
BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
sleep 1

wal-g backup-mark -b "$BACKUP"

echo "# no error expected"
wal-g backup-mark -b "$BACKUP"