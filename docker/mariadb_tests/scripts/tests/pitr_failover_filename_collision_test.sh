#!/bin/bash
set -e -o pipefail -x

. /usr/local/export_common.sh

# Check MariaDB version
if ! mariadb_version_check "10.8"; then
    echo "SKIP: This test requires MariaDB 10.8 or higher for stable GTID support"
    exit 0
fi

# Covers what pitr_failover_gtid_skip_test.sh skips: a promoted node can
# reuse the backup boundary file's name, but its byte position isn't valid.
# PRIMARY and REPLICA are rotated so both land on mysql-bin.000003.

REPLICA_DATA=/var/lib/mysql_replica
REPLICA_PORT=3307
REPLICA_SOCKET=/var/run/mysqld/mysqld_replica.sock
REPLICA_PIDFILE=/var/run/mysqld/mysqld_replica.pid
REPLICA_DSN="sbtest:@unix(${REPLICA_SOCKET})/sbtest"

mysql_replica() {
    mysql --socket="${REPLICA_SOCKET}" -u root "$@"
}

stop_replica() {
    if [ -S "${REPLICA_SOCKET}" ]; then
        mysqladmin --socket="${REPLICA_SOCKET}" -u root shutdown 2>/dev/null || true
    fi
    for _ in $(seq 1 10); do
        [ -S "${REPLICA_SOCKET}" ] || break
        sleep 1
    done
    rm -rf "${REPLICA_DATA}" "${REPLICA_SOCKET}" "${REPLICA_PIDFILE}"
}
trap stop_replica EXIT

export WALE_S3_PREFIX=s3://mariadb-pitr-failover-filename-collision
# Unlike pitr_failover_gtid_skip_test.sh, this one actually uses start-position.
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mariadb-binlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" ${WALG_MYSQL_BINLOG_START_POSITION:+--start-position=$WALG_MYSQL_BINLOG_START_POSITION} "$WALG_MYSQL_CURRENT_BINLOG" | mariadb'
export WALG_MYSQL_BINLOG_DST=/tmp/binlogs

rm -rf "${WALG_MYSQL_BINLOG_DST}"
mkdir -p "${WALG_MYSQL_BINLOG_DST}"

# ----- PRIMARY: the default instance (server_id=1, see docker/mariadb/my.cnf) -----
mariadb_installdb
service mariadb start

# ----- REPLICA: a second instance (server_id=2, log-slave-updates) -----
EMPTY_INIT_FILE=/tmp/empty_init.sql
: > "${EMPTY_INIT_FILE}"

mkdir -p /var/run/mysqld
rm -rf "${REPLICA_DATA}"
mkdir -p "${REPLICA_DATA}"
mysql_install_db --user=mysql --datadir="${REPLICA_DATA}" > /dev/null
chown -R mysql:mysql "${REPLICA_DATA}"

mariadbd --user=mysql \
    --datadir="${REPLICA_DATA}" \
    --port="${REPLICA_PORT}" \
    --socket="${REPLICA_SOCKET}" \
    --pid-file="${REPLICA_PIDFILE}" \
    --server-id=2 \
    --log-bin=mysql-bin \
    --log-bin-index="${REPLICA_DATA}/mysql-bin.index" \
    --log-slave-updates=1 \
    --gtid-domain-id=0 \
    --init-file="${EMPTY_INIT_FILE}" \
    --log-error=/var/log/mysql/replica_error.log &

for i in $(seq 1 30); do
    mysqladmin --socket="${REPLICA_SOCKET}" ping 2>/dev/null && break
    [ "$i" -eq 30 ] && (cat /var/log/mysql/replica_error.log; false)
    sleep 1
done

# ----- Wire up replication PRIMARY -> REPLICA -----
mysql -e "CREATE USER 'repl'@'localhost' IDENTIFIED BY 'replpass'"
mysql -e "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'localhost'"

mysql_replica -e "CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=3306, MASTER_USER='repl', MASTER_PASSWORD='replpass', MASTER_USE_GTID=slave_pos"
mysql_replica -e "START SLAVE"

for i in $(seq 1 30); do
    STATE=$(mysql_replica -e "SHOW SLAVE STATUS\G" 2>/dev/null | grep -cE "Slave_IO_Running: Yes|Slave_SQL_Running: Yes" || true)
    [ "$STATE" -eq 2 ] && break
    [ "$i" -eq 30 ] && (mysql_replica -e "SHOW SLAVE STATUS\G"; false)
    sleep 1
done

# Covered data lands in mysql-bin.000002.
mysql_replica -e "FLUSH LOGS"

# Two rotations so PRIMARY's boundary matches REPLICA's post-failover number.
mysql -e "FLUSH LOGS"
mysql -e "FLUSH LOGS"

# ----- PRIMARY: create the data that ends up in the backup -----
mysql -e "CREATE DATABASE IF NOT EXISTS testdb"
mysql -e "CREATE TABLE testdb.users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50), created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
mysql -e "INSERT INTO testdb.users (name) VALUES ('Alice'), ('Bob'), ('Charlie')"

SYNC_GTID=$(mysql -N -e "SELECT @@GLOBAL.gtid_current_pos")
WAIT_RESULT=$(mysql_replica -N -e "SELECT MASTER_GTID_WAIT('${SYNC_GTID}', 30)")
[ "$WAIT_RESULT" = "0" ] || (echo "ERROR: replica did not catch up in time"; mysql_replica -e "SHOW SLAVE STATUS\G"; false)

# ----- Backup from PRIMARY: boundary is mysql-bin.000003 with a real byte position -----
wal-g backup-push
FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
BOUNDARY_FILE=$(mysql -N -e "SHOW MASTER STATUS" | awk '{print $1}')
echo "First backup: $FIRST_BACKUP, boundary file: $BOUNDARY_FILE"
[ "$BOUNDARY_FILE" = "mysql-bin.000003" ] || (echo "ERROR: expected boundary file mysql-bin.000003, got $BOUNDARY_FILE"; false)

# Close REPLICA's covered file and archive it.
mysql_replica -e "FLUSH LOGS"
WALG_MYSQL_DATASOURCE_NAME="${REPLICA_DSN}" wal-g binlog-push

# ----- Failover: REPLICA is promoted -----
mysql_replica -e "STOP SLAVE"
mysql_replica -e "RESET SLAVE ALL"

# ----- New primary takes writes the old primary never sees -----
# Lands in REPLICA's mysql-bin.000003 -- same name, different server.
REPLICA_DAVID_FILE=$(mysql_replica -N -e "SHOW MASTER STATUS" | awk '{print $1}')
[ "$REPLICA_DAVID_FILE" = "mysql-bin.000003" ] || (echo "ERROR: expected REPLICA's current file to be mysql-bin.000003, got $REPLICA_DAVID_FILE"; false)
mysql_replica -e "INSERT INTO testdb.users (name) VALUES ('David')"
mysql_replica -e "FLUSH LOGS"
WALG_MYSQL_DATASOURCE_NAME="${REPLICA_DSN}" wal-g binlog-push

DT_PITR=$(date3339)

# ----- Disaster: both instances are gone, only the backup and the archived binlogs remain -----
stop_replica
trap - EXIT
mariadb_kill_and_clean_data
wal-g backup-fetch LATEST

chown -R mysql:mysql "${MYSQLDATA}"
service mariadb start || (cat /var/log/mysql/error.log && false)

mysql_set_gtid_from_backup

rm -rf "${WALG_MYSQL_BINLOG_DST:?}"/*

# 000002 skips via GTID, 000003 replays despite the name collision.
wal-g binlog-replay --since LATEST --until "$DT_PITR" --until-binlog-last-modified-time "$DT_PITR"

# ----- Verify: all four rows present, no duplicate-replay errors -----
mysql -e "SELECT COUNT(*) FROM testdb.users" | grep -q 4
mysql -e "SELECT name FROM testdb.users WHERE name='Alice'" | grep -q "Alice"
mysql -e "SELECT name FROM testdb.users WHERE name='David'" | grep -q "David"

echo "PITR failover filename-collision test completed successfully"
