#!/bin/sh
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
if [ "$?" -ne 0 ]
then
	exit 1
else
	echo "Everything is ok, diff returned 0 exit code"
fi
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
if [ "$?" -ne 0 ]
then
	exit 1
else
	echo "Everything is ok, diff returned 0 exit code"
fi
pkill -9 postgres
rm -rf $PGDATA
exit 0
