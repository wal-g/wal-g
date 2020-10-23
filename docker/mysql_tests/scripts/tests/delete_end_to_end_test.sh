#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqldeleteendtoendbucket

# initialize mysql
mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
sysbench --table-size=10 prepare
mysql -e "FLUSH LOGS"

# first backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

mysqldump sbtest > /tmp/dump_1.sql
wal-g backup-list
test "2" -eq "$(wal-g backup-list | wc -l)"
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
DT1=$(date3339)


# second backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

mysqldump sbtest > /tmp/dump_2.sql
test "3" -eq "$(wal-g backup-list | wc -l)"
SECOND_BACKUP=$(wal-g backup-list | awk 'NR==3{print $1}')
DT2=$(date3339)


# third backup
sysbench --time=3 run
wal-g backup-push
sysbench --time=3 run
mysql -e "FLUSH LOGS"
wal-g binlog-push
sleep 1

mysqldump sbtest > /tmp/dump_3.sql
test "4" -eq "$(wal-g backup-list | wc -l)"
THIRD_BACKUP=$(wal-g backup-list | awk 'NR==4{print $1}')
DT3=$(date3339)


# delete first backup
wal-g delete before FIND_FULL "$SECOND_BACKUP" --confirm
test "3" -eq "$(wal-g backup-list | wc -l)"


# test restore second
mysql_kill_and_clean_data
wal-g backup-fetch "$SECOND_BACKUP"
chown -R mysql:mysql "$MYSQLDATA"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged
wal-g binlog-replay --since "$SECOND_BACKUP" --until "$DT2"
mysqldump sbtest > /tmp/dump_2_restored.sql
diff -u /tmp/dump_2.sql /tmp/dump_2_restored.sql


# delete second backup
wal-g delete retain 1 --confirm
test "2" -eq "$(wal-g backup-list | wc -l)"


# test restore third backup
mysql_kill_and_clean_data
wal-g backup-fetch "$THIRD_BACKUP"
chown -R mysql:mysql "$MYSQLDATA"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged
wal-g binlog-replay --since "$THIRD_BACKUP" --until "$DT3"
mysqldump sbtest > /tmp/dump_3_restored.sql
diff -u /tmp/dump_3.sql /tmp/dump_3_restored.sql
