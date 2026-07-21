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
setup_wal_archiving
enable_pitr_extension

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# 1st full backup (init tables heap, ao, co)
insert_data
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,1000000)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,1000000)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"

# 2nd backup (populate the co table)
psql -p 6000 -d test -c "DELETE FROM co WHERE a>0;"
psql -p 6000 -d test -c "DELETE FROM ao WHERE a>0;"
psql -p 6000 -d test -c "VACUUM FULL;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(10000000,20000000)i;"
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(10000000,20000000)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}


stop_and_delete_cluster_dir

wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}
start_cluster
sleep 5
psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;"

psql -p 6000 -d test -c "SELECT COUNT(*) FROM ao;" | grep 10000001 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from ao table after restore"
    exit 1
fi

psql -p 6000 -d test -c "SELECT COUNT(*) FROM co;" | grep 10000001 && EXIT_STATUS=$? || EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne 0 ] ; then
    echo "Error: Failed to read from co table after restore"
    exit 1
fi


cleanup
rm ${TMP_CONFIG}
