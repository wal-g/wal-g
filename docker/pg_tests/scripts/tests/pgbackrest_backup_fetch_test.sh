set -e -x
CONFIG_FILE="/tmp/configs/pgbackrest_backup_fetch_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
PGBACKREST_CONFIG="/tmp/configs/pgbackrest_backup_fetch_config.ini"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}
 

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

archive_command="/usr/bin/timeout 600 pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups archive-push %p"
echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '${archive_command}'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh

mkdir -m 770 /tmp/pgbackrest-backups

pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups stanza-create

pgbench -i -s 5 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups backup

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w stop

s3cmd mb s3://pgbackrest-backups || echo "Bucket pgbackrest-backups already exists"
s3cmd sync /tmp/pgbackrest-backups/backup s3://pgbackrest-backups
s3cmd sync /tmp/pgbackrest-backups/archive s3://pgbackrest-backups

/tmp/scripts/drop_pg.sh
pgbackrest --stanza=main --pg1-path=${PGDATA} --repo1-path=/tmp/pgbackrest-backups restore
rm "${PGDATA}/recovery.conf"
tar --mtime='UTC 2019-01-01' --sort=name -cf /tmp/pg_data_expected.tar ${PGDATA}
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} pgbackrest backup-fetch ${PGDATA} LATEST
tar --mtime='UTC 2019-01-01' --sort=name -cf /tmp/pg_data_actual.tar ${PGDATA}

diff /tmp/pg_data_expected.tar /tmp/pg_data_actual.tar
echo "Pgbackrest and wal-g backups are the same!"

echo "restore_command = 'wal-g --config=${TMP_CONFIG} pgbackrest wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
echo "Backup success!!!!!!"

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
