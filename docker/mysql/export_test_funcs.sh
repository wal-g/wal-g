#!/usr/bin/env bash

# test tools
mysql_kill_and_clean_data() {
    service mysql stop || true
    kill -9 "$(pidof mysqld)" || true
    rm -rf "${MYSQLDATA}"/*
    rm -rf "${MYSQLDATA}"/.tmp
    rm -rf /root/.walg_mysql_binlogs_cache
}

mysql_set_gtid_purged() {
    gtids=$(tr -d '\n' < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
    echo "Gtids from backup $gtids"
    mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtids';"
}

sysbench() {
    /usr/bin/sysbench --verbosity=0 /usr/share/sysbench/oltp_insert.lua "$@"
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}

