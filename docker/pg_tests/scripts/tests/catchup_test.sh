#!/bin/bash
set -e -x

PGDATA="/var/lib/postgresql/10/main"
PGDATA_ALPHA="${PGDATA}_alpha"
PGDATA_BETA="${PGDATA}_beta"
PGDATA_BETA_1="${PGDATA}_beta_1"
ALPHA_DUMP="/tmp/alpha_dump"
ALPHA_PORT=5432
BETA_DUMP="/tmp/beta_dump"
BETA_PORT=5433

# init config
CONFIG_FILE="/tmp/configs/catchup_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cp ${CONFIG_FILE} ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

# init alpha cluster
/usr/lib/postgresql/10/bin/initdb ${PGDATA_ALPHA}
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_ALPHA} -w start
PGDATA=${PGDATA_ALPHA} /tmp/scripts/wait_while_pg_not_ready.sh

# preparation for replication
pushd ${PGDATA_ALPHA}
psql -c "CREATE ROLE repl WITH REPLICATION PASSWORD 'password' LOGIN;"
echo "host  replication  repl              127.0.0.1/32  md5" >> pg_hba.conf
echo "wal_level = replica" >> postgresql.conf
echo "wal_keep_segments = 100" >> postgresql.conf
echo "max_wal_senders = 4" >> postgresql.conf
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_ALPHA} -w restart
PGDATA=${PGDATA_ALPHA} /tmp/scripts/wait_while_pg_not_ready.sh
popd

# init beta cluster (replica of alpha)
/usr/lib/postgresql/10/bin/pg_basebackup --wal-method=stream -D ${PGDATA_BETA} -U repl -h 127.0.0.1 -p ${ALPHA_PORT}

cp -r ${PGDATA_BETA} ${PGDATA_BETA_1}

pushd ${PGDATA_BETA}
echo "port = ${BETA_PORT}" >> postgresql.conf
echo "hot_standby = on" >> postgresql.conf
cat > recovery.conf << EOF
standby_mode = 'on'
primary_conninfo = 'host=127.0.0.1 port=${ALPHA_PORT} user=repl password=password'
restore_command = 'cp ${PGDATA_BETA}/archive/%f %p'
trigger_file = '/tmp/postgresql.trigger.${BETA_PORT}'
EOF
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w start
popd

# fill database postgres
pgbench -i -s 15 -h 127.0.0.1 -p ${ALPHA_PORT} postgres

LSN=`psql -c "SELECT pg_current_wal_lsn() - '0/0'::pg_lsn;" | grep -E '[0-9]+' | head -1`

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} --mode smart -w stop

# change database postgres and dump database
pgbench -T 10 -P 1 -h 127.0.0.1 -p ${ALPHA_PORT} postgres
# create some new files
pgbench -i -s 5 -h 127.0.0.1 -p ${ALPHA_PORT} postgres
/usr/lib/postgresql/10/bin/pg_dump -h 127.0.0.1 -p ${ALPHA_PORT} -f ${ALPHA_DUMP} postgres

wal-g --config=${TMP_CONFIG} catchup-push ${PGDATA_ALPHA} --from-lsn ${LSN} 2>/tmp/stderr 1>/tmp/stdout
cat /tmp/stderr /tmp/stdout

BACKUP_NAME=`grep -oE 'base_[0-9A-Z]*' /tmp/stderr | sort -u`

wal-g --config=${TMP_CONFIG} catchup-fetch ${PGDATA_BETA} $BACKUP_NAME

pushd ${PGDATA_BETA}
echo "port = ${BETA_PORT}" >> postgresql.conf
echo "hot_standby = on" >> postgresql.conf
cat > recovery.conf << EOF
standby_mode = 'on'
primary_conninfo = 'host=127.0.0.1 port=${ALPHA_PORT} user=repl password=password'
restore_command = 'cp ${PGDATA_BETA}/archive/%f %p'
trigger_file = '/tmp/postgresql.trigger.${BETA_PORT}'
EOF
popd

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w start

/usr/lib/postgresql/10/bin/pg_dump -h 127.0.0.1 -p ${BETA_PORT} -f ${BETA_DUMP} postgres

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w stop

diff ${ALPHA_DUMP} ${BETA_DUMP}

# test catchup-send and catchup-receive
rm -rf ${PGDATA_BETA}

mv ${PGDATA_BETA_1} ${PGDATA_BETA}

wal-g catchup-receive ${PGDATA_BETA} 1337 &

wal-g --config=${TMP_CONFIG} catchup-send ${PGDATA_ALPHA} localhost:1337


pushd ${PGDATA_BETA}
echo "port = ${BETA_PORT}" >> postgresql.conf
echo "hot_standby = on" >> postgresql.conf
cat > recovery.conf << EOF
standby_mode = 'on'
primary_conninfo = 'host=127.0.0.1 port=${ALPHA_PORT} user=repl password=password'
restore_command = 'cp ${PGDATA_BETA}/archive/%f %p'
trigger_file = '/tmp/postgresql.trigger.${BETA_PORT}'
EOF
popd

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w start

/usr/lib/postgresql/10/bin/pg_dump -h 127.0.0.1 -p ${BETA_PORT} -f ${BETA_DUMP} postgres

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA_BETA} -w stop

#diff ${ALPHA_DUMP} ${BETA_DUMP}

/tmp/scripts/drop_pg.sh
rm -rf ${PGDATA_ALPHA} ${PGDATA_BETA}
