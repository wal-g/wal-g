#!/bin/sh
echo '\e[0;31m This test require some memory available to docker! \e[0m'
echo 'It runs smooth on Colima with \e[0;31m 2 CPU / 8 GB Mem \e[0m and fails on 4 GB Mem.'
set -e -x

. /tmp/tests/test_functions/pg_compat.sh

PGDATA_ALPHA="${PGDATA}_alpha"
PGDATA_BETA="${PGDATA}_beta"
# Raised from 4: PG 15 and later archive fewer segments for the same scale.
PGBENCH_SCALE=8
ALPHA_PORT=5432
BETA_PORT=5433

check_archiving_status() {
    local port=$1
    local expected_count=$2
    local timeout_minutes=${3:-10}
    local max_wait=`expr $timeout_minutes \* 60`
    local interval=5

    echo "Starting archive check, waiting for $expected_count files, timeout in $timeout_minutes minutes"

    i=0
    while [ $i -lt $max_wait ]; do
        local archived_count=`psql -h 127.0.0.1 -p $port -t -c "
            SELECT archived_count
            FROM pg_stat_archiver;" | tr -d ' '`

        if [ "$archived_count" -ge "$expected_count" ]; then
            echo "Archiving completed: $archived_count files archived"
            return 0
        fi

        local remaining_minutes=`expr \( $max_wait - $i \) / 60`
        local remaining_seconds=`expr \( $max_wait - $i \) % 60`
        echo "Waiting for archiving to complete... ($archived_count/$expected_count) - Remaining time: ${remaining_minutes}m ${remaining_seconds}s"
        sleep $interval
        i=`expr $i + $interval`
    done

    echo "Warning: Archiving did not complete after $timeout_minutes minutes"
    return 1
}

# init config
. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/wal_restore_test_config.json"

# init alpha cluster
initdb ${PGDATA_ALPHA}

# preparation for replication
cd ${PGDATA_ALPHA}
echo "host  replication  repl              127.0.0.1/32  $(hba_repl_method ${PGDATA_ALPHA})" >> pg_hba.conf
{
  echo "wal_level = replica"
  # Alpha must still keep the WAL where the two clusters diverged, for pg_rewind.
  echo "$(wal_keep_conf 32 ${PGDATA_ALPHA})"
  echo "max_wal_senders = 2"
  echo "hot_standby = on"
  echo "listen_addresses = 'localhost'"
  echo "wal_log_hints = on"

  echo "archive_mode = on"
  echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p --config=${TMP_CONFIG}'"
  echo "archive_timeout = 600"
} >> postgresql.conf

pg_ctl -D ${PGDATA_ALPHA} -w start
PGDATA=${PGDATA_ALPHA} /tmp/scripts/wait_while_pg_not_ready.sh

timeout 30 wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

psql -c "CREATE ROLE repl WITH REPLICATION PASSWORD 'password' LOGIN;"

# init beta cluster (replica of alpha)
pg_basebackup --wal-method=stream -D ${PGDATA_BETA} -U repl -h 127.0.0.1 -p ${ALPHA_PORT}

# preparation for replication
cd ${PGDATA_BETA}
{
  echo "wal_log_hints = on"
  echo "max_wal_size = 32MB"
  echo "min_wal_size = 32MB"

  echo "port = ${BETA_PORT}"
  echo "hot_standby = on"

  echo "archive_mode = on"
  echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p --config=${TMP_CONFIG}'"
  echo "archive_timeout = 600"
} >> postgresql.conf
write_standby_settings ${PGDATA_BETA} << EOF
standby_mode = 'on'
primary_conninfo = 'host=127.0.0.1 port=${ALPHA_PORT} user=repl password=password'
restore_command = 'cp ${PGDATA_BETA}/archive/%f %p'
trigger_file = '/tmp/postgresql.trigger.${BETA_PORT}'
EOF

pg_ctl -D ${PGDATA_BETA} -w start

# fill database postgres
pgbench -i -s ${PGBENCH_SCALE} -h 127.0.0.1 -p ${ALPHA_PORT} postgres

# Wait for WAL to be archived. The numbers below are minimums, not exact values,
# and they depend on PGBENCH_SCALE.
if ! check_archiving_status ${ALPHA_PORT} 6 10; then
    echo "Failed to wait for archiving completion on alpha instance"
    exit 1
fi

#                                               db       table            conn_port    row_count
# pgbench_accounts has 100k rows for each scale unit
/tmp/scripts/wait_while_replication_complete.sh postgres pgbench_accounts ${BETA_PORT} $((PGBENCH_SCALE * 100000))

pg_ctl -D ${PGDATA_ALPHA} -m fast -w stop

pg_ctl -D ${PGDATA_BETA} -w promote

pgbench -i -s ${PGBENCH_SCALE} -h 127.0.0.1 -p ${BETA_PORT} postgres

# NOTES: Must wait for archiving completion before stopping the database for two reasons:
#
# 1. Prevent pg_rewind failure:
#    - When using "pg_ctl -m fast stop", some PostgreSQL processes will exit early,
#      but postmaster and archiver remain running
#    - The archiver process continues to call wal-g wal-push to upload WAL files
#    - During archiving, .ready files are created and deleted(renamed)
#    - This will cause subsequent pg_rewind(archiver is still running) to fail with error:
#      "could not open source file .../archive_status/XXX.ready: No such file or directory"
#
# 2. Ensure complete testing of wal-g wal-restore:
#    - Without waiting, WAL files might not be uploaded to S3 at all
#    - This would cause wal-g wal-restore to exit early with "No WAL files to restore",
#      preventing thorough testing of the wal-restore functionality
if ! check_archiving_status ${BETA_PORT} 8 10; then
    echo "Failed to wait for archiving completion on beta instance"
    exit 1
fi

pg_ctl -D ${PGDATA_BETA} -m fast -W stop

pg_ctl -D ${PGDATA_ALPHA} -w start
PGDATA=${PGDATA_ALPHA} /tmp/scripts/wait_while_pg_not_ready.sh


pgbench -i -s ${PGBENCH_SCALE} -h 127.0.0.1 -p ${ALPHA_PORT} postgres

# Wait for WAL files to be archived. Not strictly required, but better to do(see above notes):
if ! check_archiving_status ${ALPHA_PORT} 8 10; then
    echo "Failed to wait for archiving completion on alpha instance"
    exit 1
fi

pg_ctl -D ${PGDATA_ALPHA} -m fast -w stop

# for more info to log
ls "${PGDATA_BETA}/pg_wal"

timeout 30 wal-g --config=${TMP_CONFIG} wal-restore ${PGDATA_ALPHA} ${PGDATA_BETA}

pg_rewind -D ${PGDATA_ALPHA} --source-pgdata=${PGDATA_BETA}

/tmp/scripts/drop_pg.sh
rm -rf ${PGDATA_ALPHA} ${PGDATA_BETA}
