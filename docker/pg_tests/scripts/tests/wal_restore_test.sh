#!/bin/sh
set -e -x

PGDATA="/var/lib/postgresql/10/main"
PGDATA_ALPHA="${PGDATA}_alpha"
PGDATA_BETA="${PGDATA}_beta"
ALPHA_PORT=5432
BETA_PORT=5433

# init config
CONFIG_FILE="/tmp/configs/wal_restore_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cp ${CONFIG_FILE} ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

# init alpha cluster
/usr/lib/postgresql/10/bin/initdb ${PGDATA_ALPHA}

# preparation for replication
cd ${PGDATA_ALPHA}
echo "host  replication  repl              127.0.0.1/32  md5" >> pg_hba.conf
{
  echo "wal_level = replica"
  echo "wal_keep_segments = 100"
  echo "max_wal_senders = 2"
  echo "hot_standby = on"
  echo "listen_addresses = 'localhost'"
  echo "wal_log_hints = on"

  echo "archive_mode = on"
  echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p --config=${TMP_CONFIG}'"
  echo "archive_timeout = 600"
} >> postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_ALPHA} -w start
PGDATA=${PGDATA_ALPHA} /tmp/scripts/wait_while_pg_not_ready.sh

timeout 30 wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

psql -c "CREATE ROLE repl WITH REPLICATION PASSWORD 'password' LOGIN;"

# init beta cluster (replica of alpha)
/usr/lib/postgresql/10/bin/pg_basebackup --wal-method=stream -D ${PGDATA_BETA} -U repl -h 127.0.0.1 -p ${ALPHA_PORT}

# preparation for replication
cd ${PGDATA_BETA}
{
  echo "wal_log_hints = on"

  echo "port = ${BETA_PORT}"
  echo "hot_standby = on"

  echo "archive_mode = on"
  echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p --config=${TMP_CONFIG}'"
  echo "archive_timeout = 600"
} >> postgresql.conf
cat > recovery.conf << EOF
standby_mode = 'on'
primary_conninfo = 'host=127.0.0.1 port=${ALPHA_PORT} user=repl password=password'
restore_command = 'cp ${PGDATA_BETA}/archive/%f %p'
trigger_file = '/tmp/postgresql.trigger.${BETA_PORT}'
EOF

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w start

# fill database postgres
pgbench -i -s 10   -h 127.0.0.1 -p ${ALPHA_PORT} postgres

#                                               db       table            conn_port    row_count
/tmp/scripts/wait_while_replication_complete.sh postgres pgbench_accounts ${ALPHA_PORT} 1000000 # 10 * 100000, 10 is value of -s in pgbench
# script above waits only one table, so just in case sleep
sleep 3

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_ALPHA} -m fast -w stop
sleep 7

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w promote

pgbench -i -s 10 -h 127.0.0.1 -p ${BETA_PORT} postgres

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -m fast -W stop
sleep 3

# for more info to log
ls "${PGDATA_BETA}/pg_wal"

rm "${PGDATA_BETA}/pg_wal/00000002000000000000000C"
rm "${PGDATA_BETA}/pg_wal/00000002000000000000000D"
rm "${PGDATA_BETA}/pg_wal/00000002000000000000000E"

timeout 30 wal-g --config=${TMP_CONFIG} wal-restore ${PGDATA_BETA} ${PGDATA_ALPHA}
sleep 3

/usr/lib/postgresql/10/bin/pg_rewind -D ${PGDATA_BETA} --source-pgdata=${PGDATA_ALPHA}
