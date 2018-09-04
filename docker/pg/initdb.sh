#!/bin/sh
set -e
/usr/lib/postgresql/10/bin/pg_ctl -D $PGDATA -w start
pgbench -i -s 10 postgres
pg_dumpall -f /tmp/dump1
wal-g backup-push $PGDATA
pkill -9 postgres
rm -rf $PGDATA
wal-g backup-fetch $PGDATA LATEST
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > $PGDATA/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D $PGDATA -w start
pg_dumpall -f /tmp/dump2
diff /tmp/dump1 /tmp/dump2
pgbench -i -s 20 postgres
pg_dumpall -f /tmp/dump3
wal-g backup-push $PGDATA
pkill -9 postgres
rm -rf $PGDATA
wal-g backup-fetch $PGDATA LATEST
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > $PGDATA/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D $PGDATA -w start
pg_dumpall -f /tmp/dump4
diff /tmp/dump3 /tmp/dump4
pkill -9 postgres
rm -rf $PGDATA
exit 0
