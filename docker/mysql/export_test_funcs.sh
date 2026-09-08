#!/usr/bin/env bash

# test tools
mysql_cache_server_version() {
    mysql_detected_server_version=${WALG_MYSQL_TEST_SERVER_VERSION:-}
    if [ -z "$mysql_detected_server_version" ]; then
        mysql_detected_server_version=$(mysql --batch --skip-column-names -e "SELECT VERSION()") || {
            echo "Failed to query MySQL server version" >&2
            return 2
        }
    fi
    case "$mysql_detected_server_version" in
        *[![:alnum:].+~_-]*) echo "Invalid MySQL server version: $mysql_detected_server_version" >&2; return 2 ;;
    esac
    if ! printf '%s\n' "$mysql_detected_server_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][[:alnum:].~_-]+)?$'; then
        echo "Invalid MySQL server version: $mysql_detected_server_version" >&2
        return 2
    fi
    WALG_MYSQL_TEST_SERVER_VERSION=$mysql_detected_server_version
    export WALG_MYSQL_TEST_SERVER_VERSION
}

mysql_version_at_least() {
    # 0: at least the requested version; 1: older; 2: detection failed.
    mysql_cache_server_version || return 2

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
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 22; then
        mysql -e "STOP REPLICA"
    else
        mysql -e "STOP SLAVE"
    fi
}

mysql_start_replica() {
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 22; then
        mysql -e "START REPLICA"
    else
        mysql -e "START SLAVE"
    fi
}

mysql_reset_replica_all() {
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 22; then
        mysql -e "RESET REPLICA ALL"
    else
        mysql -e "RESET SLAVE ALL"
    fi
}

mysql_show_replica_status() {
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 22; then
        mysql -e "SHOW REPLICA STATUS\G"
    else
        mysql -e "SHOW SLAVE STATUS\G"
    fi
}

mysql_change_replication_source() {
    mysql_cache_server_version || return 2
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
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 26; then
        mysql -e "SET GLOBAL replica_net_timeout = $1"
    else
        mysql -e "SET GLOBAL slave_net_timeout = $1"
    fi
}

mysql_set_replica_transaction_retries() {
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 0 26; then
        mysql -e "SET GLOBAL replica_transaction_retries = $1"
    else
        mysql -e "SET GLOBAL slave_transaction_retries = $1"
    fi
}

mysql_reset_binary_logs_and_gtids() {
    mysql_cache_server_version || return 2
    if mysql_version_at_least 8 4 0; then
        mysql -e "RESET BINARY LOGS AND GTIDS"
    else
        mysql -e "RESET MASTER"
    fi
}

# Run directly: recent Percona packages only ship systemd units, whereas the
# integration containers have neither systemd nor an init.d service.
mysql_start() {
    unset WALG_MYSQL_TEST_SERVER_VERSION
    mkdir -p /var/run/mysqld /var/log/mysql || return 1
    chown mysql:mysql /var/run/mysqld /var/log/mysql || return 1
    mysqld --user=mysql --pid-file=/var/run/mysqld/mysqld.pid \
        --log-error=/var/log/mysql/error.log &
    mysql_test_pid=$!
    mysql_start_attempt=0
    while [ "$mysql_start_attempt" -lt 60 ]; do
        if ! kill -0 "$mysql_test_pid" 2>/dev/null; then
            wait "$mysql_test_pid" || true
            cat /var/log/mysql/error.log >&2
            return 1
        fi
        if mysqladmin --no-defaults --user=root --protocol=socket --connect-timeout=1 ping >/dev/null 2>&1; then
            return 0
        fi
        mysql_start_attempt=$((mysql_start_attempt + 1))
        sleep 1
    done
    echo "MySQL did not become ready" >&2
    cat /var/log/mysql/error.log >&2
    mysql_stop || true
    return 1
}

mysql_stop() {
    # The suite runner is a different shell from individual tests: use the
    # explicit pid file to stop a server left running by a completed test.
    if [ -f /var/run/mysqld/mysqld.pid ]; then
        mysql_test_pid=$(cat /var/run/mysqld/mysqld.pid) || return 1
    fi
    case "${mysql_test_pid:-}" in
        '') return 0 ;;
        *[!0-9]*|0|1) echo "Invalid mysqld PID: $mysql_test_pid" >&2; return 1 ;;
    esac
    if kill -0 "$mysql_test_pid" 2>/dev/null; then
        kill "$mysql_test_pid" || return 1
        mysql_stop_attempt=0
        while kill -0 "$mysql_test_pid" 2>/dev/null; do
            # A server orphaned by a finished test may remain a zombie until
            # the container's PID 1 reaps it. It no longer owns data files.
            if [ -f "/proc/$mysql_test_pid/stat" ] &&
                [ "$(awk '{print $3}' "/proc/$mysql_test_pid/stat")" = Z ]; then
                break
            fi
            mysql_stop_attempt=$((mysql_stop_attempt + 1))
            if [ "$mysql_stop_attempt" -ge 60 ]; then
                echo "MySQL did not stop within 60 seconds" >&2
                kill -9 "$mysql_test_pid" || return 1
                wait "$mysql_test_pid" 2>/dev/null || true
                return 1
            fi
            sleep 1
        done
    fi
    wait "$mysql_test_pid" 2>/dev/null || true
    mysql_test_pid=
    unset WALG_MYSQL_TEST_SERVER_VERSION
}

mysql_initialize_and_start() {
    mysql_binary_version=$(mysqld --version) || return 1
    case "$mysql_binary_version" in
        *" Ver 5.7."*)
            mysqld --initialize --user=mysql --init-file=/etc/mysql/init.sql || return 1
            mysql_start
            ;;
        *)
            mysqld --initialize-insecure --user=mysql || return 1
            mysql_start || return 1
            mysql --no-defaults --user=root < /etc/mysql/init.sql
            ;;
    esac
}

mysql_kill_and_clean_data() {
    mysql_stop || return 1
    # These tests only own the container's MySQL data directory.
    if [ "${MYSQLDATA:-}" != /var/lib/mysql ]; then
        echo "Refusing to clean unexpected MYSQLDATA: ${MYSQLDATA:-}" >&2
        return 1
    fi
    rm -rf /var/lib/mysql/* /var/lib/mysql/.tmp
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
