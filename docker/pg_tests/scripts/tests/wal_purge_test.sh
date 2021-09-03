#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/wal_purge_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 1 postgres
pg_dumpall -f /tmp/dump1
sleep 1
# push permanent backup
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --permanent

PERMANENT_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==2{print $1}')

# add some WALs
pgbench -i -s 3 postgres
sleep 1

# make two non-permanent backups
for i in 1 2
do
    pgbench -i -s 2 postgres
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

FIRST_NON_PERMANENT_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==3{print $1}')

# delete the first non-permanent backup
wal-g --config=${TMP_CONFIG} delete target ${FIRST_NON_PERMANENT_BACKUP} --confirm

# should delete WALs in ranges (0, PERMANENT_BACKUP) and (PERMANENT_BACKUP, second non-permanent backup)
wal-g --config=${TMP_CONFIG} wal-purge --confirm

FIRST_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==2{print $1}')

if [ "$PERMANENT_BACKUP" != "$FIRST_BACKUP" ];
then
    echo "oh no! wal-purge deleted the permanent backup!"
    exit 1
fi

# run wal-verify to make sure WAL-G didn't delete anything useful
wal-g wal-verify integrity --config=${TMP_CONFIG} > /tmp/wal_verify_output

if grep -q "FAILURE" /tmp/wal_verify_output;
then
  echo "wal-verify check failure!"
  cat /tmp/wal_verify_output
  exit 1
fi

# try to restore the permanent backup
/tmp/scripts/drop_pg.sh

sleep 1
wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} $PERMANENT_BACKUP

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

echo "success!!!!!!"

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
