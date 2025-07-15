#!/usr/bin/env bash

# common wal-g settings
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=xbstream --user=sbtest --host=localhost --datadir=${MYSQLDATA}"
export WALG_STREAM_RESTORE_COMMAND="mbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=${MYSQLDATA}"
export WALG_MYSQL_CHECK_GTIDS=False


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
    mysql_install_db > /dev/null && chown -R mysql:mysql $MYSQLDATA
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
    mysql -e "STOP ALL SLAVES; SET GLOBAL gtid_slave_pos='$gtids';"
}