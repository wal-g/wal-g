#!/usr/bin/env bash

# common wal-g settings
#export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
#export WALG_STREAM_CREATE_COMMAND="mariadb-backup --backup --stream=xbstream --user=sbtest --host=localhost --datadir=${MYSQLDATA} --ssl=0"
#export WALG_STREAM_RESTORE_COMMAND="mbstream -x -C ${MYSQLDATA}"
#export WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariadb-backup --prepare --target-dir=${MYSQLDATA}"
#export WALG_MYSQL_CHECK_GTIDS=False
export WALG_COMPRESSION_METHOD=zstd
#export WALG_LOG_LEVEL=DEVEL


export MARIADB_DB_HOST='localhost'
export MARIADB_DB_PORT='3306'
export MARIADB_DB_DATABASE='sbtest'
export MARIADB_DB_USERNAME='sbtest'
export MARIADB_DB_PASSWORD=''
export MARIADB_DB_PATH='/var/lib/mysql'
export CONN_OPTIONS="--user=${MARIADB_DB_USERNAME} --password=${MARIADB_DB_PASSWORD} --host=${MARIADB_DB_HOST} --port=${MARIADB_DB_PORT} --ssl=0"
export WALG_MYSQL_DATASOURCE_NAME="${MARIADB_DB_USERNAME}:${MARIADB_DB_PASSWORD}@tcp(${MARIADB_DB_HOST}:${MARIADB_DB_PORT})/${MARIADB_DB_DATABASE}"
export WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=mbstream --datadir=${MARIADB_DB_PATH} ${CONN_OPTIONS}"
export WALG_STREAM_RESTORE_COMMAND="mbstream -x -C ${MARIADB_DB_PATH}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=${MARIADB_DB_PATH} ${CONN_OPTIONS}"
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mariadb-binlog --stop-datetime="${WALG_MYSQL_BINLOG_END_TS}" "${WALG_MYSQL_CURRENT_BINLOG}" | mariadb'
export WALG_MYSQL_BINLOG_DST='/tmp'
#export WALG_COMPRESSION_METHOD='brotli'
export WALG_DELTA_MAX_STEPS='6'
export WALG_UPLOAD_CONCURRENCY='10'
export WALG_DISK_RATE_LIMIT='41943040'
export WALG_NETWORK_RATE_LIMIT='10485760'

# test tools
mariadb_kill_and_clean_data() {
    # MariaDB service is weired - it returns error on service stop. Repeat it until success
    while ! service mariadb stop
    do
      echo "Stopping MariaDB... Try again"
      sleep 1
    done

    rm -rf "${MYSQLDATA}"/*
    rm -rf "${MYSQLDATA}"/.tmp
    rm -rf /root/.walg_mysql_binlogs_cache
}

mariadb_installdb() {
    /usr/bin/mariadb-install-db > /dev/null && chown -R mysql:mysql $MYSQLDATA
    mkdir -p /var/log/mysql || true
    chown -R mysql:mysql /var/log/mysql
}

sysbench() {
    # shellcheck disable=SC2068
    /usr/bin/sysbench --db-driver=mysql --verbosity=0 /usr/share/sysbench/oltp_insert.lua $@
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}

mysql_set_gtid_from_backup() {
    gtids=$(tail -n 1 < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
    echo "GTIDs from backup $gtids"
    mariadb -e "STOP ALL SLAVES; SET GLOBAL gtid_slave_pos='$gtids';"
}