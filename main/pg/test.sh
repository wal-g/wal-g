#!/bin/bash
set -e
rm -rf test test2
pkill -9 postgres || true
pkill -9 wal-g || true
export PG=$HOME/project/bin

$PG/initdb test
$PG/pg_ctl -D test start

$PG/pgbench -i postgres
$PG/psql -c "checkpoint" postgres

$PG/pg_basebackup --wal-method=stream -D test2
rm test2/backup_label

$PG/pgbench postgres
$PG/psql -c "checkpoint" postgres
$PG/pgbench postgres

$PG/pg_controldata test
$PG/pg_controldata test2

./pg catchup-receive test2 1337 &
 PGDATABASE=postgres ./pg catchup-send test localhost:1337