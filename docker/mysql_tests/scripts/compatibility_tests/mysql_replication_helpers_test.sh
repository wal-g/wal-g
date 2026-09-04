#!/usr/bin/env bash
set -euo pipefail

# shellcheck disable=SC1091
. "$(dirname "$0")/../../../mysql/export_test_funcs.sh"

calls_file=$(mktemp)
trap 'rm -f "$calls_file"' EXIT

mysql() {
    printf '%s\n' "$*" >>"$calls_file"
}

assert_commands() {
    test_version=$1
    expected_commands=$2

    export WALG_MYSQL_TEST_SERVER_VERSION=$test_version
    : >"$calls_file"

    mysql_stop_replica
    mysql_reset_replica_all
    mysql_set_replica_net_timeout 10
    mysql_set_replica_transaction_retries 20
    mysql_change_replication_source "127.0.0.1" 9306 "walg" "walgpwd" 1 2 86400
    mysql_start_replica
    mysql_show_replica_status
    mysql_reset_binary_logs_and_gtids

    actual_commands=$(cat "$calls_file")
    if [ "$actual_commands" != "$expected_commands" ]; then
        printf 'Unexpected commands for MySQL %s\n' "$test_version" >&2
        diff -u <(printf '%s\n' "$expected_commands") <(printf '%s\n' "$actual_commands") >&2 || true
        return 1
    fi
}

assert_commands 5.7.44 $'-e STOP SLAVE\n-e RESET SLAVE ALL\n-e SET GLOBAL slave_net_timeout = 10\n-e SET GLOBAL slave_transaction_retries = 20\n-e CHANGE MASTER TO MASTER_HOST=\'127.0.0.1\', MASTER_PORT=9306, MASTER_USER=\'walg\', MASTER_PASSWORD=\'walgpwd\', MASTER_AUTO_POSITION=1, MASTER_CONNECT_RETRY=1, MASTER_HEARTBEAT_PERIOD=2, MASTER_RETRY_COUNT=86400\n-e START SLAVE\n-e SHOW SLAVE STATUS\\G\n-e RESET MASTER'

assert_commands 8.0.22 $'-e STOP REPLICA\n-e RESET REPLICA ALL\n-e SET GLOBAL slave_net_timeout = 10\n-e SET GLOBAL slave_transaction_retries = 20\n-e CHANGE MASTER TO MASTER_HOST=\'127.0.0.1\', MASTER_PORT=9306, MASTER_USER=\'walg\', MASTER_PASSWORD=\'walgpwd\', MASTER_AUTO_POSITION=1, MASTER_CONNECT_RETRY=1, MASTER_HEARTBEAT_PERIOD=2, MASTER_RETRY_COUNT=86400\n-e START REPLICA\n-e SHOW REPLICA STATUS\\G\n-e RESET MASTER'

assert_commands 8.0.23 $'-e STOP REPLICA\n-e RESET REPLICA ALL\n-e SET GLOBAL slave_net_timeout = 10\n-e SET GLOBAL slave_transaction_retries = 20\n-e CHANGE REPLICATION SOURCE TO SOURCE_HOST=\'127.0.0.1\', SOURCE_PORT=9306, SOURCE_USER=\'walg\', SOURCE_PASSWORD=\'walgpwd\', SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=1, SOURCE_HEARTBEAT_PERIOD=2, SOURCE_RETRY_COUNT=86400\n-e START REPLICA\n-e SHOW REPLICA STATUS\\G\n-e RESET MASTER'

assert_commands 8.0.26 $'-e STOP REPLICA\n-e RESET REPLICA ALL\n-e SET GLOBAL replica_net_timeout = 10\n-e SET GLOBAL replica_transaction_retries = 20\n-e CHANGE REPLICATION SOURCE TO SOURCE_HOST=\'127.0.0.1\', SOURCE_PORT=9306, SOURCE_USER=\'walg\', SOURCE_PASSWORD=\'walgpwd\', SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=1, SOURCE_HEARTBEAT_PERIOD=2, SOURCE_RETRY_COUNT=86400\n-e START REPLICA\n-e SHOW REPLICA STATUS\\G\n-e RESET MASTER'

assert_commands 8.4.0 $'-e STOP REPLICA\n-e RESET REPLICA ALL\n-e SET GLOBAL replica_net_timeout = 10\n-e SET GLOBAL replica_transaction_retries = 20\n-e CHANGE REPLICATION SOURCE TO SOURCE_HOST=\'127.0.0.1\', SOURCE_PORT=9306, SOURCE_USER=\'walg\', SOURCE_PASSWORD=\'walgpwd\', SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=1, SOURCE_HEARTBEAT_PERIOD=2, SOURCE_RETRY_COUNT=86400\n-e START REPLICA\n-e SHOW REPLICA STATUS\\G\n-e RESET BINARY LOGS AND GTIDS'

assert_commands 9.7.1-rc1 $'-e STOP REPLICA\n-e RESET REPLICA ALL\n-e SET GLOBAL replica_net_timeout = 10\n-e SET GLOBAL replica_transaction_retries = 20\n-e CHANGE REPLICATION SOURCE TO SOURCE_HOST=\'127.0.0.1\', SOURCE_PORT=9306, SOURCE_USER=\'walg\', SOURCE_PASSWORD=\'walgpwd\', SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=1, SOURCE_HEARTBEAT_PERIOD=2, SOURCE_RETRY_COUNT=86400\n-e START REPLICA\n-e SHOW REPLICA STATUS\\G\n-e RESET BINARY LOGS AND GTIDS'
