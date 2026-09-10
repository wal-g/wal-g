#!/bin/bash
set -e -x

. /tmp/tests/test_functions/pg_compat.sh

PGBACKREST_CONFIG="/tmp/configs/pgbackrest_backup_fetch_config.ini"

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/pgbackrest_backup_fetch_config.json"

initdb ${PGDATA}

archive_command="/usr/bin/timeout 600 pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups archive-push %p"
echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '${archive_command}'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 60" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

mkdir -m 770 /tmp/pgbackrest-backups

pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups stanza-create

pgbench -i -s 5 postgres
pgbench -c 2 -T 60 &
pgbench_pid=$!

sleep 1
pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups backup &
pgbackrest_pid=$!
wait $pgbench_pid
switch_wal postgres
wait $pgbackrest_pid
dump_all /tmp/dump1

pg_ctl -D ${PGDATA} -w stop

s3cmd mb s3://pgbackrest-backups || echo "Bucket pgbackrest-backups already exists"
s3cmd sync /tmp/pgbackrest-backups/backup s3://pgbackrest-backups
s3cmd sync /tmp/pgbackrest-backups/archive s3://pgbackrest-backups

/tmp/scripts/drop_pg.sh
pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups restore

# During restore pgbackrest writes its own recovery settings: recovery.conf on
# PG < 12, recovery.signal and postgresql.auto.conf on PG >= 12. wal-g does not
# write them, so these files are excluded from both archives.
TAR_EXCLUDES="--exclude=recovery.conf --exclude=recovery.signal --exclude=postgresql.auto.conf"

tar --mtime='UTC 2019-01-01' --sort=name ${TAR_EXCLUDES} -cf /tmp/pg_data_expected.tar ${PGDATA}
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} pgbackrest backup-fetch ${PGDATA} LATEST
tar --mtime='UTC 2019-01-01' --sort=name ${TAR_EXCLUDES} -cf /tmp/pg_data_actual.tar ${PGDATA}

# Compare the unpacked files, not the tar archives: comparing archives only says
# that they are different, which does not help when the test fails.
mkdir -p /tmp/cmp/expected /tmp/cmp/actual
tar -xf /tmp/pg_data_expected.tar -C /tmp/cmp/expected
tar -xf /tmp/pg_data_actual.tar -C /tmp/cmp/actual
# During restore pgBackRest (2.48 and later) writes an invalid checkpoint LSN
# into pg_control, but wal-g restores the file as it was, so the two files can
# never be equal. All other pg_control fields are still compared below.
diff -r -x pg_control /tmp/cmp/expected /tmp/cmp/actual

pg_controldata "/tmp/cmp/expected${PGDATA}" | grep -v '^Latest checkpoint location:' > /tmp/ctl_expected
pg_controldata "/tmp/cmp/actual${PGDATA}"   | grep -v '^Latest checkpoint location:' > /tmp/ctl_actual
diff /tmp/ctl_expected /tmp/ctl_actual

echo "Pgbackrest and wal-g backups are the same!"

echo "restore_command = 'wal-g --config=${TMP_CONFIG} pgbackrest wal-fetch \"%f\" \"%p\"'" | write_recovery_settings
pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
dump_all /tmp/dump2

compare_dumps /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
echo "Backup success!!!!!!"

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
