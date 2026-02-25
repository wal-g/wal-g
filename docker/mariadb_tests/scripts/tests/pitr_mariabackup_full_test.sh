#!/bin/bash
set -e -o pipefail -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mariadb_pitr_mariabackup_full
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
export WALG_MYSQL_BINLOG_DST=/tmp/binlogs

mariadb_installdb
service mysql start

# Create initial data
mysql -e "CREATE DATABASE testdb"
mysql -e "CREATE TABLE testdb.users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50), created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
mysql -e "INSERT INTO testdb.users (name) VALUES ('Alice'), ('Bob')"

# First full backup
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
echo "First backup: $FIRST_BACKUP"

# Add more data and flush logs
mysql -e "INSERT INTO testdb.users (name) VALUES ('Charlie')"
mysql -e "CREATE TABLE testdb.products (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50), price DECIMAL(10,2))"
mysql -e "INSERT INTO testdb.products (name, price) VALUES ('Keyboard', 75.00), ('Mouse', 25.50)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

# Record PITR timestamp
sleep 2
DT_PITR=$(date3339)
sleep 2

# Add data after PITR point (this should NOT be restored)
mysql -e "INSERT INTO testdb.users (name) VALUES ('David')"
mysql -e "INSERT INTO testdb.products (name, price) VALUES ('Monitor', 299.99)"
mysql -e "FLUSH LOGS"
wal-g binlog-push

# Verify data before disaster
mysql -e "SELECT COUNT(*) FROM testdb.users" | grep -q 5
mysql -e "SELECT COUNT(*) FROM testdb.products" | grep -q 3

# Simulate disaster
mysql -e "DROP DATABASE testdb"

# Kill and restore
mariadb_kill_and_clean_data
wal-g backup-fetch LATEST

# Get GTIDs from backup
cat /var/lib/mysql/xtrabackup_binlog_info
gtids=$(tail -n 1 /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
echo "GTIDs from backup: $gtids"

chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)

# Reset GTIDs
mysql -e "STOP ALL SLAVES; SET GLOBAL gtid_slave_pos='$gtids';" || true
mysql -e "SET GLOBAL gtid_slave_pos='$gtids';"

# Apply binlogs until PITR point
wal-g binlog-replay --since LATEST --until "$DT_PITR"

# Verify PITR restore
mysql -e "SELECT COUNT(*) FROM testdb.users" | grep -q 3  # Only Alice, Bob, Charlie (not David)
mysql -e "SELECT COUNT(*) FROM testdb.products" | grep -q 2  # Only Keyboard, Mouse (not Monitor)

# Verify specific data
mysql -e "SELECT name FROM testdb.users WHERE name='Alice'" | grep -q "Alice"
mysql -e "SELECT name FROM testdb.users WHERE name='Charlie'" | grep -q "Charlie"
mysql -e "SELECT name FROM testdb.users WHERE name='David'" && exit 1 || true  # David should NOT exist

echo "PITR test completed successfully"
