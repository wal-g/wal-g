#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/ghost_table_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 5 postgres
psql -c "create table ghost (a int, b int);"
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
pgbench -i -s 5 postgres
psql -c "insert into ghost values (1, 2);"
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
psql -c "drop table ghost;"
pgbench -i -s 5 postgres
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} stop 
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pgbench -i -s 5 postgres
psql -c "create table ghost (a int, b int);"
psql -c "insert into ghost values (3, 4);"

pg_dumpall -f /tmp/dump1
sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Ghost table backup success!!!!!!"
