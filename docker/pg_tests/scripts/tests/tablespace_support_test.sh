#!/bin/sh
set -e -x

export WALG_FILE_PREFIX=file://localhost/tmp
export WALG_LOG_DESTINATION=stderr

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

mkdir /tmp/spaces
mkdir /tmp/spaces/space
mkdir /tmp/spaces/space2
psql -c "create tablespace space location '/tmp/spaces/space';"
psql -c "create table cinemas (id integer, name text, location text) tablespace space;"
psql -c "insert into cinemas (id, name, location) values (1, 'Inseption', 'USA');"
psql -c "insert into cinemas (id, name, location) values (2, 'Taxi', 'France');"
psql -c "insert into cinemas (id, name, location) values (3, 'Spirited Away', 'Japan');"
psql -c "create tablespace space2 location '/tmp/spaces/space2';"
psql -c "create table series (id integer, name text) tablespace space2;"
psql -c "insert into series (id, name) values (1, 'Game of Thrones');"
psql -c "insert into series (id, name) values (2, 'Black mirror');"
psql -c "insert into series (id, name) values (3, 'Sherlock');"
psql -c "create table users (id integer, name text, password text);"
psql -c "insert into users (id, name, password) values(1, 'ismirn0ff', 'password');"
psql -c "insert into users (id, name, password) values(2, 'tinsane', 'qwerty');"
psql -c "insert into users (id, name, password) values(3, 'godjan', 'g0djan');"
psql -c "insert into users (id, name, password) values(4, 'x4m', 'borodin');"
pg_dumpall -f /tmp/dump1
sleep 1

wal-g backup-push ${PGDATA}
pkill -9 postgres

cd /tmp/basebackups_005
cp -r /tmp/spaces /tmp/spaces_backup

rm -rf /tmp/spaces/*
rm -rf ${PGDATA}

wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2
diff -r /tmp/spaces_backup /tmp/spaces

../scripts/drop_pg.sh

echo "Tablespaces work!!!"
