#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/delete_garbage_test_config.json"
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
for _ in 1 2
do
    pgbench -i -s 2 postgres
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

FIRST_NON_PERMANENT_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==3{print $1}')

# backup the first non-permanent backup sentinel and remove it from the storage
# to emulate some partially deleted backup
# then try to delete the garbage (without the --confirm flag)

wal-g st cat "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" > "/tmp/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --config=${TMP_CONFIG}
wal-g st rm "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --config=${TMP_CONFIG}

# check that ARCHIVES mode works
wal-g delete garbage ARCHIVES --config=${TMP_CONFIG} > /tmp/delete_garbage_archives_output 2>&1
if ! grep -q "wal_005" /tmp/delete_garbage_archives_output;
then
  echo "wal-g delete garbage ARCHIVES did not delete any of the wal_005/* files!"
  cat /tmp/delete_garbage_archives_output
  exit 1
fi
if grep -q "basebackups_005" /tmp/delete_garbage_archives_output;
then
  echo "wal-g delete garbage ARCHIVES deleted the basebackups_005/* files!"
  cat /tmp/delete_garbage_archives_output
  exit 1
fi

# check that BACKUPS mode works
wal-g delete garbage BACKUPS --config=${TMP_CONFIG} > /tmp/delete_garbage_backups_output 2>&1
if ! grep -q "basebackups_005" /tmp/delete_garbage_backups_output;
then
  echo "wal-g delete garbage BACKUPS did not delete any of the basebackups_005/* files!"
  cat /tmp/delete_garbage_backups_output
  exit 1
fi
if grep -q "wal_005" /tmp/delete_garbage_backups_output;
then
  echo "wal-g delete garbage BACKUPS deleted the wal_005/* files!"
  cat /tmp/delete_garbage_backups_output
  exit 1
fi

# check that default mode works
wal-g delete garbage --config=${TMP_CONFIG} > /tmp/delete_garbage_default_output 2>&1
if ! grep -q "basebackups_005" /tmp/delete_garbage_default_output;
then
  echo "wal-g delete garbage did not delete any of the basebackups_005/* files!"
  cat /tmp/delete_garbage_default_output
  exit 1
fi
if ! grep -q "wal_005" /tmp/delete_garbage_default_output;
then
  echo "wal-g delete garbage did not delete any of the wal_005/* files!"
  cat /tmp/delete_garbage_default_output
  exit 1
fi

# restore the backup sentinel
wal-g st put "/tmp/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" "basebackups_005/${FIRST_NON_PERMANENT_BACKUP}_backup_stop_sentinel.json" --no-compress --no-encrypt --config=${TMP_CONFIG}

# delete the first non-permanent backup
wal-g --config=${TMP_CONFIG} delete target "${FIRST_NON_PERMANENT_BACKUP}" --confirm

# should delete WALs in ranges (0, PERMANENT_BACKUP) and (PERMANENT_BACKUP, second non-permanent backup)
wal-g --config=${TMP_CONFIG} delete garbage --confirm

FIRST_BACKUP=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==2{print $1}')

if [ "$PERMANENT_BACKUP" != "$FIRST_BACKUP" ];
then
    echo "oh no! delete garbage deleted the permanent backup!"
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
