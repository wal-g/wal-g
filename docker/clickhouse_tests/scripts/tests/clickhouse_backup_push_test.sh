#!/bin/sh
set -e -x

export WALG_CLICKHOUSE_BACKUP_PATH="/var/lib/clickhouse/backup"
export WALG_CLICKHOUSE_CREATE_BACKUP="clickhouse-backup create -c /usr/lib/clickhouse-backup/config.yml"
export WALG_FILE_PREFIX='/tmp/wal-g-test-data'

export S3_STORAGE_CLASS="STANDARD"

service clickhouse-server start --config-file=/etc/clickhouse-server/config.xml

sleep 10

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tests"
clickhouse-client --query "CREATE TABLE tests.backup_push
(
    user_id UInt64,
    name String
)
ENGINE = MergeTree() order by user_id settings index_granularity = 2;"
clickhouse-client --query "INSERT INTO tests.backup_push VALUES (1, 'alex')"

wal-g backup-push
