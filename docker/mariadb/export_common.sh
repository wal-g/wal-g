#!/usr/bin/env bash

# common wal-g settings
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=xbstream --user=sbtest --host=localhost --datadir=${MYSQLDATA}"
export WALG_STREAM_RESTORE_COMMAND="mbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=${MYSQLDATA}"


# test tools
mariadb_kill_and_clean_data() {
    kill -9 `pidof mysqld` || true
    rm -rf "${MYSQLDATA}"/*
    rm -rf /root/.walg_mysql_binlogs_cache
}

sysbench() {
    # shellcheck disable=SC2068
    /usr/bin/sysbench --db-driver=mysql --verbosity=0 /usr/share/sysbench/oltp_insert.lua $@
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}
