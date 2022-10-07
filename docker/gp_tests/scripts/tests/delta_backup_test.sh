#!/bin/bash
set -e -x
CONFIG_FILE="/tmp/configs/delta_backup_test_config.json"

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
sleep 3
setup_wal_archiving
enable_pitr_extension

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# 1st full backup (init tables heap, ao, co)
insert_data
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"

# 2nd backup (populate the co table)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"

# 3rd backup (populate the ao table)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

# 4th backup (should be full)
run_backup_logged ${TMP_CONFIG} ${PGDATA}

# delete the first full backup
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==3{print $1}' | cut -f 1 -d " ")

wal-g --config=${TMP_CONFIG} backup-list

wal-g --config=${TMP_CONFIG} delete before $backup_name --confirm

# show the storage objects (useful for debug)
wal-g st ls -r --config=${TMP_CONFIG}

stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}
start_cluster

psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;" | grep 30 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from ao table after restore"
    exit 1
fi

psql -p 6000 -d test -c "SELECT COUNT(*) FROM co;" | grep 30 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from co table after restore"
    exit 1
fi

# 5th backup (should be full)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

# 6th backup (delta)
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,10)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,10)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}

stop_and_delete_cluster_dir

# delete the first delta chain
backup_name=$(wal-g --config=${TMP_CONFIG} backup-list | awk 'NR==5 {print$1}' | cut -f 1 -d " ")
wal-g --config=${TMP_CONFIG} delete before FIND_FULL $backup_name --confirm

# should successfully restore the second delta chain
wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}
start_cluster

psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;" | grep 50 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from ao table after restore"
    exit 1
fi

psql -p 6000 -d test -c "SELECT COUNT(*) FROM co;" | grep 50 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from co table after restore"
    exit 1
fi

cleanup
rm ${TMP_CONFIG}
