#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/copy_backup_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

mkdir /tmp/copy_backup_test_storage
TO_CONFIG_FILE="/tmp/configs/copy_backup_to_test_config.json"
TO_TMP_CONFIG="/tmp/configs/to_tmp_config.json"
cat ${TO_CONFIG_FILE} > ${TO_TMP_CONFIG}
echo "," >> ${TO_TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TO_TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

sleep 1
echo $WALG_DELTA_MAX_STEPS

# push a backup
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push "${PGDATA}"
wal-g --config=${TMP_CONFIG} backup-list

# copy backup with backup-name
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")
wal-g copy --backup-name=${backup_name} --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG} --without-history
copied_backup_name=`wal-g --config=${TO_TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

if [ $backup_name != $copied_backup_name ];
then
    echo "Copying backup failed"
    exit 2
fi

# push some more backups
for i in 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# remember a backup...
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

# ...and push backups again
for i in 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# copy that backup
wal-g copy --backup-name=${backup_name} --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG}
copied_backup_name=$(wal-g --config=${TO_TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")

# check if backup copied
if [ "$last_backup_name" != "$copied_last_backup_name" ];
then
    echo "Copying backup failed"
    exit 2
fi

/tmp/scripts/drop_pg.sh

echo "Copying backup test success!!!!!!"
