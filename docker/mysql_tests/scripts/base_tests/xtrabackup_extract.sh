#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALG_LOG_LEVEL=DEVEL
export WALE_S3_PREFIX=s3://mysqlxtrabackupextractbucket

export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA} \
    --compress=zstd"
export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA} --decompress"

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

# add compressed tables with 2^20 rows:
mysql -e "CREATE TABLE sbtest.mytest (id int NOT NULL AUTO_INCREMENT, val varchar(80) DEFAULT NULL, PRIMARY KEY (id)) ENGINE=InnoDB COMPRESSION='zlib'"
mysql -e "INSERT INTO sbtest.mytest(val) VALUES ('aaa')"
mysql -e "INSERT INTO sbtest.mytest(val) VALUES ('bbb')"
for i in $(seq 1 4); do
  mysql -e "INSERT INTO sbtest.mytest(val) (SELECT concat(a.val, b.val) FROM sbtest.mytest as a cross join sbtest.mytest as b )"
done
sleep 1

mysql -e 'FLUSH LOGS'

# mysqldump sbtest > /tmp/dump_before_backup

wal-g xtrabackup-push

mysql_kill_and_clean_data

FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
wal-g st get "basebackups_005/${FIRST_BACKUP}/stream.br" stream.xb

cat <<EOF
##########
# test "xb extract" without decompression
##########
EOF
mkdir -p wout
wal-g xb extract stream.xb wout/
find wout -type f | sort -u | xargs cat | md5sum > wout.sum

mkdir -p xout
cat stream.xb | xbstream -x -C xout
find wout -type f | sort -u | xargs cat | md5sum > xout.sum

diff wout.sum xout.sum

rm -rf wout xout

cat <<EOF
##########
# test "xb extract" with --decompress
##########
EOF
mkdir -p wout
wal-g xb extract stream.xb wout/ --decompress
find wout -type f | sort -u | xargs cat | md5sum > wout.sum

mkdir -p xout
cat stream.xb | xbstream -x -C xout --decompress
find wout -type f | sort -u | xargs cat | md5sum > xout.sum

diff wout.sum xout.sum

cat <<EOF
##########
# check punch holes
##########
EOF

cat > list_holes.py << EOF
#!/usr/bin/env python3

import sys
import os

path = sys.argv[1]

fd = os.open(path, os.O_RDONLY)

ranges = []
hole_start = 0
hole_end = 0
while True:
    end = os.fstat(fd).st_size
    # if there is no more holes -> result is at the end of file
    hole_start = os.lseek(fd, hole_end, os.SEEK_HOLE)
    if hole_start == end:
        ranges.append("{}-{}".format(hole_start, end))
        os.close(fd)
        print('{} {}'.format(path, '.'.join(ranges)))
        exit(0)
    hole_end = os.lseek(fd, hole_start, os.SEEK_DATA)
    ranges.append("{}-{}".format(hole_start, hole_end))
EOF

find wout -type f | sort -u | xargs python3 list_holes.py > wout.holes
find xout -type f | sort -u | xargs python3 list_holes.py > xout.holes

diff wout.holes xout.holes

#chown -R mysql:mysql $MYSQLDATA
#service mysql start || (cat /var/log/mysql/error.log && false)
#mysql_set_gtid_purged
#mysqldump sbtest > /tmp/dump_after_restore
#diff /tmp/dump_before_backup /tmp/dump_after_restore
