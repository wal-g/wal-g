#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/delete_before_permanent_full_test_config.json"

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

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# push first backup as permanent
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push --permanent ${PGDATA}
wal-g --config=${TMP_CONFIG} backup-list
permanent_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`

# push a few more impermanent backups
for i in 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done
wal-g --config=${TMP_CONFIG} backup-list

# delete all backups
last_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`
wal-g --config=${TMP_CONFIG} delete before $last_backup_name --confirm
wal-g --config=${TMP_CONFIG} backup-list

# check that permanent backup still exists
first_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | sed '2q;d' | cut -f 1 -d " "`
if [ $first_backup_name != $permanent_backup_name ];
then
    echo $permanent_backup_name > /tmp/before_mark
    echo $first_backup_name > /tmp/after_mark
    echo "permanent backup does not exist after deletion"
    diff /tmp/before_mark /tmp/after_mark
fi
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Delete before permanent full success!!!!!!"