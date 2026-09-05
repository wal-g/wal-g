#!/usr/bin/env bash

# test tools
mysql_cache_server_version() {
    if [ -z "${WALG_MYSQL_TEST_SERVER_VERSION:-}" ]; then
        WALG_MYSQL_TEST_SERVER_VERSION=$(mysql --batch --skip-column-names -e "SELECT VERSION()")
        export WALG_MYSQL_TEST_SERVER_VERSION
    fi
}

mysql_version_at_least() {
    mysql_cache_server_version

    mysql_current_version_key=$(
        printf '%s\n' "$WALG_MYSQL_TEST_SERVER_VERSION" |
            awk -F '[.-]' '{printf "%d%03d%03d\n", $1, $2, $3}'
    )
    mysql_required_version_key=$(
        printf '%s.%s.%s\n' "$1" "$2" "$3" |
            awk -F '[.-]' '{printf "%d%03d%03d\n", $1, $2, $3}'
    )

    [ "$mysql_current_version_key" -ge "$mysql_required_version_key" ]
}

mysql_stop_replica() {
    if mysql_version_at_least 8 0 22; then
        mysql -e "STOP REPLICA"
    else
        mysql -e "STOP SLAVE"
    fi
}

mysql_start_replica() {
    if mysql_version_at_least 8 0 22; then
        mysql -e "START REPLICA"
    else
        mysql -e "START SLAVE"
    fi
}

mysql_reset_replica_all() {
    if mysql_version_at_least 8 0 22; then
        mysql -e "RESET REPLICA ALL"
    else
        mysql -e "RESET SLAVE ALL"
    fi
}

mysql_show_replica_status() {
    if mysql_version_at_least 8 0 22; then
        mysql -e "SHOW REPLICA STATUS\G"
    else
        mysql -e "SHOW SLAVE STATUS\G"
    fi
}

mysql_change_replication_source() {
    mysql_replication_host=$1
    mysql_replication_port=$2
    mysql_replication_user=$3
    mysql_replication_password=$4
    mysql_replication_connect_retry=${5:-}
    mysql_replication_heartbeat_period=${6:-}
    mysql_replication_retry_count=${7:-}

    if mysql_version_at_least 8 0 23; then
        mysql_replication_options="SOURCE_HOST='$mysql_replication_host', SOURCE_PORT=$mysql_replication_port, SOURCE_USER='$mysql_replication_user', SOURCE_PASSWORD='$mysql_replication_password', SOURCE_AUTO_POSITION=1"
        if [ -n "$mysql_replication_connect_retry" ]; then
            mysql_replication_options="$mysql_replication_options, SOURCE_CONNECT_RETRY=$mysql_replication_connect_retry"
        fi
        if [ -n "$mysql_replication_heartbeat_period" ]; then
            mysql_replication_options="$mysql_replication_options, SOURCE_HEARTBEAT_PERIOD=$mysql_replication_heartbeat_period"
        fi
        if [ -n "$mysql_replication_retry_count" ]; then
            mysql_replication_options="$mysql_replication_options, SOURCE_RETRY_COUNT=$mysql_replication_retry_count"
        fi
        mysql -e "CHANGE REPLICATION SOURCE TO $mysql_replication_options"
    else
        mysql_replication_options="MASTER_HOST='$mysql_replication_host', MASTER_PORT=$mysql_replication_port, MASTER_USER='$mysql_replication_user', MASTER_PASSWORD='$mysql_replication_password', MASTER_AUTO_POSITION=1"
        if [ -n "$mysql_replication_connect_retry" ]; then
            mysql_replication_options="$mysql_replication_options, MASTER_CONNECT_RETRY=$mysql_replication_connect_retry"
        fi
        if [ -n "$mysql_replication_heartbeat_period" ]; then
            mysql_replication_options="$mysql_replication_options, MASTER_HEARTBEAT_PERIOD=$mysql_replication_heartbeat_period"
        fi
        if [ -n "$mysql_replication_retry_count" ]; then
            mysql_replication_options="$mysql_replication_options, MASTER_RETRY_COUNT=$mysql_replication_retry_count"
        fi
        mysql -e "CHANGE MASTER TO $mysql_replication_options"
    fi
}

mysql_set_replica_net_timeout() {
    if mysql_version_at_least 8 0 26; then
        mysql -e "SET GLOBAL replica_net_timeout = $1"
    else
        mysql -e "SET GLOBAL slave_net_timeout = $1"
    fi
}

mysql_set_replica_transaction_retries() {
    if mysql_version_at_least 8 0 26; then
        mysql -e "SET GLOBAL replica_transaction_retries = $1"
    else
        mysql -e "SET GLOBAL slave_transaction_retries = $1"
    fi
}

mysql_reset_binary_logs_and_gtids() {
    if mysql_version_at_least 8 4 0; then
        mysql -e "RESET BINARY LOGS AND GTIDS"
    else
        mysql -e "RESET MASTER"
    fi
}

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
    mysql_reset_binary_logs_and_gtids
    mysql -e "SET @@GLOBAL.GTID_PURGED='$gtids';"
}

sysbench() {
    /usr/bin/sysbench --verbosity=0 --db-driver=mysql /usr/share/sysbench/oltp_insert.lua "$@"
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}
