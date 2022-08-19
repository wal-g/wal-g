#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mariadb_pitr_xtrabackup
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
export WALG_MYSQL_BINLOG_DST=/tmp

mysql_install_db > /dev/null
service mysql start

# add some data to database, so
sysbench --table-size=10 prepare
sysbench --time=5 run

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


# PiTR restore after LATEST backup
mariadb_kill_and_clean_data
wal-g backup-fetch LATEST

cat /var/lib/mysql/xtrabackup_binlog_info

chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
# For backups made from replicas we should update GTIDs. Test our reset logic here.
# https://mariadb.com/kb/en/gtid/#setting-up-a-new-replica-from-a-backup
mysql_set_gtid_from_backup


# WARNING:
# PiTR with GTIDs supported by MariaDB 10.8+ versions:
# https://fromdual.com/sites/default/files/fosdem_2022_pitr.pdf
# https://mariadb.com/kb/en/mariadb-1080-release-notes/#mysqlbinlog-gtid-support
#
# Before 10.8 they recommend one of the options:
# * replay with binlog and position
# * replicate from alive MariaDB server
#
# It seems that MariaDB won't skip transactions it already seen: gtid_strict_mode will not prevent from applying wrong transactions

#wal-g binlog-replay --since LATEST --until "$DT1"
#mysqldump sbtest > /tmp/dump_after_pitr
#grep -w 'testpitr01' /tmp/dump_after_pitr
#grep -w 'testpitr02' /tmp/dump_after_pitr
#grep -w 'testpitr03' /tmp/dump_after_pitr
#! grep -w 'testpitr04' /tmp/dump_after_pitr
#
#
## PiTR restore from first full backup
#mariadb_kill_and_clean_data
#wal-g backup-fetch $FIRST_BACKUP
#chown -R mysql:mysql $MYSQLDATA
#service mysql start || (cat /var/log/mysql/error.log && false)
#mysql_set_gtid_from_backup
#wal-g binlog-replay --since $FIRST_BACKUP --until "$DT1"
#mysqldump sbtest > /tmp/dump_after_pitr
#grep -w 'testpitr01' /tmp/dump_after_pitr
#grep -w 'testpitr02' /tmp/dump_after_pitr
#grep -w 'testpitr03' /tmp/dump_after_pitr
#! grep -w 'testpitr04' /tmp/dump_after_pitr
