#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/ghost_table_test_config.json"
tmp/scripts/wrap_config_file.sh ${CONFIG_FILE}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${CONFIG_FILE} wal-push %p && mkdir -p /tmp/deltas/$(basename %p)'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 10 postgres
psql -c "create table ghost (a int, b int);"
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}
pgbench -i -s 10 postgres
psql -c "insert into ghost values (1, 2);"
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}
psql -c "drop table ghost;"
pgbench -i -s 10 postgres
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} stop 
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}

pgbench -i -s 10 postgres
psql -c "create table ghost (a int, b int);"
psql -c "insert into ghost values (3, 4);"

pg_dumpall -f /tmp/dump1
sleep 1
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}

tmp/scripts/drop_pg.sh

wal-g --config=${CONFIG_FILE} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${CONFIG_FILE} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

tmp/scripts/drop_pg.sh

echo "Ghost table backup success!!!!!!"
