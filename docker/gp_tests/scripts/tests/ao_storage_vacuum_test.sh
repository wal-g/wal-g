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

# 1st full backup (init tables ao, co)
psql -p 6000 -c "DROP DATABASE IF EXISTS test"
psql -p 6000 -c "CREATE DATABASE test"
psql -p 6000 -d test -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d test -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,1000000)i;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,1000000)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA} "--full"
var=$(wal-g --config=${TMP_CONFIG} st ls segments_005/seg0/basebackups_005/aosegments/ | grep aoseg | wc -l)
if [ "$var" -ne 3 ] ; then
    echo "Error: expected 3 aoseg files but found $var"
    exit 1
fi

# 2nd backup (populate ao, co tables without deleting)
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1000000,2000000)i;"
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1000000,2000000)i;"
psql -p 6000 -d test -c "BEGIN; INSERT INTO ao select i, i FROM generate_series(10000000,20000000)i; SELECT pg_sleep(20); ROLLBACK;" &
psql -p 6000 -d test -c "BEGIN; INSERT INTO co select i, i FROM generate_series(10000000,20000000)i; SELECT pg_sleep(20); ROLLBACK;" &
sleep 5
run_backup_logged ${TMP_CONFIG} ${PGDATA}
sleep 15
var=$(wal-g --config=${TMP_CONFIG} st ls segments_005/seg0/basebackups_005/aosegments/ | grep aoseg | wc -l)
if [ "$var" -ne 6 ] ; then
    echo "Error: expected 6 aoseg files but found $var"
    exit 1
fi
var=$(wal-g --config=${TMP_CONFIG} st ls segments_005/seg0/basebackups_005/aosegments/ | grep _D_ | wc -l)
if [ "$var" -ne 3 ] ; then
    echo "Error: expected 3 aoseg delta files but found $var"
    exit 1
fi

# 3rd backup (repopulate ao and co tables after delete and vacuum)
psql -p 6000 -d test -c "DELETE FROM co WHERE a>0;"
psql -p 6000 -d test -c "DELETE FROM ao WHERE a>0;"
psql -p 6000 -d test -c "VACUUM FULL;"
psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(10000000,20000000)i;"
psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(10000000,20000000)i;"
run_backup_logged ${TMP_CONFIG} ${PGDATA}
var=$(wal-g --config=${TMP_CONFIG} st ls segments_005/seg0/basebackups_005/aosegments/ | grep aoseg | wc -l)
if [ "$var" -ne 12 ] ; then
    echo "Error: expected 12 aoseg files but found $var"
    exit 1
fi
# backup must not upload any new delta files since they chnged completely
var=$(wal-g --config=${TMP_CONFIG} st ls segments_005/seg0/basebackups_005/aosegments/ | grep _D_ | wc -l)
if [ "$var" -ne 3 ] ; then
    echo "Error: expected 3 aoseg delta files but found $var"
    exit 1
fi

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
