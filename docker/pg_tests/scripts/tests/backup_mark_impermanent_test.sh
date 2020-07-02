#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/backup_mark_impermanent_test_config.json"

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

wal-g --config=${TMP_CONFIG} backup-list --detail
sleep 1
echo $WALG_DELTA_MAX_STEPS

# push first backup as permanent
pgbench -i -s 1 postgres &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push --permanent ${PGDATA}
wal-g --config=${TMP_CONFIG} backup-list
backup_name=`wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " "`
first_backup_status=`wal-g --config=${TMP_CONFIG} backup-list --detail | tail -n 1 | egrep -o -e "true" -e "false"`

wal-g --config=${TMP_CONFIG} backup-mark -i $backup_name
last_backup_status=`wal-g --config=${TMP_CONFIG} backup-list --detail | tail -n 1 | egrep -o -e "true" -e "false"`

if [ $first_backup_status = $last_backup_status ];
then
    echo "Wrong backup status"
    exit 2
fi

# push a few more impermanent backups
for i in 2 3
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

first_backup_name=`wal-g --config=${TMP_CONFIG} backup-list | sed '2q;d' | cut -f 1 -d " "`
if [ $first_backup_name = $backup_name ];
then
    echo "impermanent backup does exist after deletion"
    exit 2
fi
/tmp/scripts/drop_pg.sh

echo "Backup mark impermanent test success!!!!!!"
