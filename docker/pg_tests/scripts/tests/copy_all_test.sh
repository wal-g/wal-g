#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/copy_all_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

mkdir /tmp/copy_all_test_storage
TO_CONFIG_FILE="/tmp/configs/copy_all_to_test_config.json"
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
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
wal-g --config=${TMP_CONFIG} backup-list
backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

# copy all
wal-g copy --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG}
copied_backup_name=`wal-g --config=${TO_TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

if [ $backup_name != $copied_backup_name ];
then
    echo "Copying all backups (when there are only 1 backup) failed"
    exit 2
fi

# push a few more backups
for i in 2 3
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done
wal-g --config=${TMP_CONFIG} backup-list

# copy all again
wal-g copy --from=${TMP_CONFIG} --to=${TO_TMP_CONFIG}

# save backup names to temp files
wal-g --config=${TMP_CONFIG} backup-list | cut -f 1 -d " " | sort > /tmp/actual_backup_list
wal-g --config=${TO_TMP_CONFIG} backup-list | cut -f 1 -d " " | sort > /tmp/copied_backup_list
# and count lines in diff
lines_count=`diff /tmp/actual_backup_list /tmp/copied_backup_list | wc -l`

if [ lines_count > 0 ];
then
    echo "Copying all backups failed"
    exit 2
fi

/tmp/scripts/drop_pg.sh

echo "Copy all backups test success!!!!!!"
