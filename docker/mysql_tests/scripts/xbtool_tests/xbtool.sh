#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALG_LOG_LEVEL=DEVEL
export WALG_COMPRESSION_METHOD=zstd
export WALE_S3_PREFIX=s3://mysql8-xbtool-bucket


export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA} \
    --compress=zstd"
export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA} --decompress"

mysql_kill_and_clean_data

mysql_initialize_and_start

# add compressed tables with 2^20 rows:
mysql -e "CREATE TABLE sbtest.mytest (id int NOT NULL AUTO_INCREMENT, val varchar(80) DEFAULT NULL, PRIMARY KEY (id)) ENGINE=InnoDB COMPRESSION='zlib'"
mysql -e "INSERT INTO sbtest.mytest(val) VALUES ('aaa')"
mysql -e "INSERT INTO sbtest.mytest(val) VALUES ('bbb')"
for i in $(seq 1 4); do
  mysql -e "INSERT INTO sbtest.mytest(val) (SELECT concat(a.val, b.val) FROM sbtest.mytest as a cross join sbtest.mytest as b )"
done
sleep 1

mysql -e 'FLUSH LOGS'

wal-g xtrabackup-push

mysql_kill_and_clean_data

FIRST_BACKUP=$(wal-g backup-list | awk 'NR==2{print $1}')
wal-g get-stream "${FIRST_BACKUP}" stream.xb

cat <<EOF
##########
# test "xb extract" without decompression
##########
EOF
mkdir -p wout
wal-g xb extract stream.xb --data-dir wout/

mkdir -p xout
cat stream.xb | xbstream -x -C xout

diff -r wout xout

rm -rf wout xout

cat <<EOF
##########
# test "xb extract" with --decompress
##########
EOF
rm -rf wout
mkdir -p wout
wal-g xb extract stream.xb --data-dir wout/ --decompress

rm -rf xout
mkdir -p xout
cat stream.xb | xbstream -x -C xout --decompress

diff -r wout xout
