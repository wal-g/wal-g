#!/bin/sh
set -e -x
  CONFIG_FILE="/tmp/configs/delete_target_delta_test_config.json"
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

# create one base backup and one increment
for _ in 1 2
do
    pgbench -i -s 1 postgres &
    sleep 1
    wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
done

# remember the name of the first increment
FIRST_INCREMENT=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'END {print $1}')

# make the second full backup
pgbench -i -s 1 postgres & sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --full

# make the increment from the second full backup
pgbench -i -s 1 postgres & sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

# remember the backup-list output with two full backups and two increments (in each of two storages).
# later in the test we create new backups which should be deleted so lists should be identical
lines_before_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_before_delete

# now make increment from the FIRST_INCREMENT, which will be deleted later
pgbench -i -s 1 postgres & sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --delta-from-name ${FIRST_INCREMENT}
INCREMENT_TO_DELETE=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'END {print $1}')

# make the increment from the INCREMENT_TO_DELETE
pgbench -i -s 1 postgres & sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --delta-from-name ${INCREMENT_TO_DELETE}

# make one more increment from the INCREMENT_TO_DELETE
pgbench -i -s 1 postgres & sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --delta-from-name ${INCREMENT_TO_DELETE}

# delete the INCREMENT_TO_DELETE, should keep only the first full backup w/ first increment and the second full backup w/ second increment
wal-g --config=${TMP_CONFIG} delete target ${INCREMENT_TO_DELETE} --confirm

lines_after_delete=`wal-g --config=${TMP_CONFIG} backup-list | wc -l`
wal-g --config=${TMP_CONFIG} backup-list > /tmp/list_after_delete

if [ $(($lines_before_delete)) -ne $lines_after_delete ];
then
    echo $(($lines_before_delete)) > /tmp/before_delete
    echo $lines_after_delete > /tmp/after_delete
    echo "Wrong number of deleted lines"
    diff /tmp/before_delete /tmp/after_delete
fi

# ensure all backups which we weren't going to delete still exist after performing deletion
xargs -I {} grep {} /tmp/list_before_delete </tmp/list_after_delete

/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
