#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlpitrxtrabackuppositionbucket
# Uses both WALG_MYSQL_BINLOG_START_POSITION and WALG_MYSQL_BINLOG_LAST_GTID,
# read from the backup sentinel and set by wal-g for the boundary binlog file.
# shellcheck disable=SC2016
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" ${WALG_MYSQL_BINLOG_START_POSITION:+--start-position="$WALG_MYSQL_BINLOG_START_POSITION"} ${WALG_MYSQL_BINLOG_LAST_GTID:+--exclude-gtids="$WALG_MYSQL_BINLOG_LAST_GTID"} "$WALG_MYSQL_CURRENT_BINLOG" | mysql'

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

mysql -e "CREATE DATABASE IF NOT EXISTS testdb"
mysql -e "CREATE TABLE testdb.users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50), created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
mysql -e "INSERT INTO testdb.users (name) VALUES ('Alice'), ('Bob')"

# Backup while this data is still in the open binlog file, so the boundary
# lands inside mysql-bin.000001 -- no earlier file to skip, replay relies on
# --start-position/--exclude-gtids within this one file instead.
sleep 1
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
echo "First backup: $FIRST_BACKUP"

mysql -e "INSERT INTO testdb.users (name) VALUES ('Charlie')"
mysql -e "CREATE TABLE testdb.products (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50), price DECIMAL(10,2))"
mysql -e "INSERT INTO testdb.products (name, price) VALUES ('Keyboard', 75.00), ('Mouse', 25.50)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

sleep 2
DT_PITR=$(date3339)
sleep 2

# Data after the PITR point -- must not be restored.
mysql -e "INSERT INTO testdb.users (name) VALUES ('David')"
mysql -e "INSERT INTO testdb.products (name, price) VALUES ('Monitor', 299.99)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql -e "SELECT COUNT(*) FROM testdb.users" | grep -q 4
mysql -e "SELECT COUNT(*) FROM testdb.products" | grep -q 3

# Disaster
mysql -e "DROP DATABASE testdb"
mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql "${MYSQLDATA}"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

wal-g binlog-replay --since LATEST --until "$DT_PITR" --until-binlog-last-modified-time "$DT_PITR"

# Alice/Bob/Charlie survive, no duplicate-key errors from replaying
# mysql-bin.000001 from its start; David does not exist.
mysql -e "SELECT COUNT(*) FROM testdb.users" | grep -q 3
mysql -e "SELECT COUNT(*) FROM testdb.products" | grep -q 2
mysql -e "SELECT name FROM testdb.users WHERE name='Alice'" | grep -q "Alice"
mysql -e "SELECT name FROM testdb.users WHERE name='Charlie'" | grep -q "Charlie"
! mysql -e "SELECT name FROM testdb.users WHERE name='David'" 2>/dev/null | grep -q "David"

echo "PITR xtrabackup position/GTID test completed successfully"
