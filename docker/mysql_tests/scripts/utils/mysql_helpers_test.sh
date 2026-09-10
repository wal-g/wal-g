#!/bin/sh
# Run without a database: sh mysql_helpers_test.sh /path/to/export_test_funcs.sh
set -eu

# shellcheck source=docker/mysql/export_test_funcs.sh
. "${1:-/usr/local/export_test_funcs.sh}"

fail() { echo "$*" >&2; exit 1; }

mysql() {
    if [ "$*" = "--batch --skip-column-names -e SELECT VERSION()" ]; then
        printf '%s\n' "$mock_version"
        return "$mock_query_status"
    fi
    last_sql=$*
}

assert_mysql_args() {
    mock_version=$1
    expected_args=$2
    shift 2
    mock_query_status=0
    last_sql=
    unset WALG_MYSQL_TEST_SERVER_VERSION
    "$@"
    test "$last_sql" = "$expected_args" || fail "Unexpected mysql arguments for $mock_version: $last_sql"
}

assert_sql() {
    assert_version=$1
    expected_sql=$2
    shift 2
    assert_mysql_args "$assert_version" "-e $expected_sql" "$@"
}

for version in 5.7.44-48 8.0.21; do
    assert_sql "$version" "STOP SLAVE" mysql_stop_replica
    assert_sql "$version" "START SLAVE" mysql_start_replica
    assert_sql "$version" "RESET SLAVE ALL" mysql_reset_replica_all
    assert_mysql_args "$version" '--vertical -e SHOW SLAVE STATUS' mysql_show_replica_status
done
for version in 8.0.22 8.4.8-8 9.7.1-1; do
    assert_sql "$version" "STOP REPLICA" mysql_stop_replica
    assert_sql "$version" "START REPLICA" mysql_start_replica
    assert_sql "$version" "RESET REPLICA ALL" mysql_reset_replica_all
    assert_mysql_args "$version" '--vertical -e SHOW REPLICA STATUS' mysql_show_replica_status
done
for version in 5.7.44-48 8.0.22; do
    assert_sql "$version" "CHANGE MASTER TO MASTER_HOST='host', MASTER_PORT=9306, MASTER_USER='user', MASTER_PASSWORD='pwd', MASTER_AUTO_POSITION=1, MASTER_SSL=0, MASTER_CONNECT_RETRY=1, MASTER_HEARTBEAT_PERIOD=2, MASTER_RETRY_COUNT=3" mysql_change_replication_source host 9306 user pwd 1 2 3
done
for version in 8.0.23 8.4.11-11 9.7.1-1; do
    assert_sql "$version" "CHANGE REPLICATION SOURCE TO SOURCE_HOST='host', SOURCE_PORT=9306, SOURCE_USER='user', SOURCE_PASSWORD='pwd', SOURCE_AUTO_POSITION=1, SOURCE_SSL=0" mysql_change_replication_source host 9306 user pwd
    assert_sql "$version" "CHANGE REPLICATION SOURCE TO SOURCE_HOST='host', SOURCE_PORT=9306, SOURCE_USER='user', SOURCE_PASSWORD='pwd', SOURCE_AUTO_POSITION=1, SOURCE_SSL=0, SOURCE_CONNECT_RETRY=1, SOURCE_HEARTBEAT_PERIOD=2, SOURCE_RETRY_COUNT=3" mysql_change_replication_source host 9306 user pwd 1 2 3
done
assert_sql 8.0.25 "SET GLOBAL slave_net_timeout = 10" mysql_set_replica_net_timeout 10
assert_sql 8.0.26 "SET GLOBAL replica_net_timeout = 10" mysql_set_replica_net_timeout 10
assert_sql 8.0.25 "SET GLOBAL slave_transaction_retries = 10" mysql_set_replica_transaction_retries 10
assert_sql 8.0.26 "SET GLOBAL replica_transaction_retries = 10" mysql_set_replica_transaction_retries 10
assert_sql 8.0.44 "RESET MASTER" mysql_reset_binary_logs_and_gtids
assert_sql 8.4.0 "RESET BINARY LOGS AND GTIDS" mysql_reset_binary_logs_and_gtids
assert_sql 9.7.1-1 "RESET BINARY LOGS AND GTIDS" mysql_reset_binary_logs_and_gtids

# A successfully cached version must not be queried again.
mock_query_status=1
mysql_stop_replica
test "$last_sql" = "-e STOP REPLICA" || fail "Version cache was not reused"

for mock_query_status in 0 1; do
    for mock_version in '' invalid 9.7 '9.7.1
garbage' 9.7.1-1; do
        if [ "$mock_query_status" = 0 ] && [ "$mock_version" = 9.7.1-1 ]; then
            continue
        fi
        for helper in mysql_version_at_least mysql_stop_replica mysql_start_replica \
            mysql_reset_replica_all mysql_show_replica_status mysql_change_replication_source \
            mysql_set_replica_net_timeout mysql_set_replica_transaction_retries mysql_reset_binary_logs_and_gtids; do
            unset WALG_MYSQL_TEST_SERVER_VERSION
            last_sql=
            # Deliberately call inside if: errexit is disabled throughout the
            # helper and must not be relied on to propagate the query error.
            if "$helper" 8 4 0 pwd 2>/dev/null; then
                fail "$helper accepted a failed/invalid version query"
            else
                status=$?
                test "$status" = 2 || fail "$helper returned $status instead of detection failure"
            fi
            test -z "$last_sql" || fail "$helper ran SQL after detection failed: $last_sql"
            test -z "${WALG_MYSQL_TEST_SERVER_VERSION:-}" || fail "$helper cached a failed query"
        done
    done
done

mock_version=5.7.44
mock_query_status=0
unset WALG_MYSQL_TEST_SERVER_VERSION
if mysql_version_at_least 8 4 0; then
    fail "5.7 was treated as >= 8.4"
else
    test "$?" = 1 || fail "Old version was treated as a detection failure"
fi

# Mock process/filesystem operations too: exercise the bounded startup and
# shutdown loops without touching a local MySQL installation.
(
    mkdir() { :; }
    chown() { :; }
    cat() {
        # Ignore any real pid file left by package installation in the image.
        if [ "$1" = /var/run/mysqld/mysqld.pid ]; then
            printf '%s\n' "$mysql_test_pid"
        fi
    }
    sleep() { :; }
    mysqld() { :; }
    service() { fail "MySQL helpers must not use service"; }
    mysqladmin() {
        mock_ping_count=$((mock_ping_count + 1))
        test "$mock_ping_count" -ge "$mock_ready_after"
    }
    kill() {
        if [ "$1" = -0 ]; then
            test "$mock_alive" = 1
        else
            mock_alive=0
        fi
    }

    mock_alive=1
    mock_ping_count=0
    mock_ready_after=3
    mysql_start
    test "$mock_ping_count" = 3 || fail "Startup did not wait for readiness"
    mysql_stop
    test "$mock_alive" = 0 || fail "Shutdown did not terminate mysqld"

    mock_alive=0
    mock_ping_count=0
    if mysql_start 2>/dev/null; then
        fail "Startup accepted a dead process"
    fi
    test "$mock_ping_count" = 0 || fail "Startup pinged a dead process"

    mock_alive=1
    mock_ping_count=0
    mock_ready_after=61
    if mysql_start 2>/dev/null; then
        fail "Startup accepted a server that never became ready"
    fi
    test "$mock_ping_count" = 60 || fail "Startup timeout was not bounded"
    test "$mock_alive" = 0 || fail "Timed-out startup left mysqld running"
)

echo "MySQL helper tests passed"
