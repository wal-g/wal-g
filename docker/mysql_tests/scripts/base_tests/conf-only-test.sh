#!/bin/sh
set -e -x

. /usr/local/export_test_funcs.sh

# https://github.com/wal-g/wal-g/issues/790
## ensure correct conf option will overwrite this trash
#export AWS_ENDPOINT=we23i902309239

cat > /root/conf.yaml <<EOH
WALE_S3_PREFIX: s3://mysqlconfonly
AWS_ENDPOINT: http://s3:9000
AWS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
WALG_S3_MAX_PART_SIZE: 5242880

WALG_STREAM_RESTORE_COMMAND: "xbstream -x -C ${MYSQLDATA}"
WALG_MYSQL_BACKUP_PREPARE_COMMAND: "xtrabackup --prepare --target-dir=${MYSQLDATA}"
WALG_MYSQL_BINLOG_REPLAY_COMMAND: 'mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
WALG_MYSQL_BINLOG_DST: /tmp
WALG_MYSQL_DATASOURCE_NAME: sbtest:@/sbtest
WALG_STREAM_CREATE_COMMAND: "xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"
EOH

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
mysql mysql -e 'create table testt1(i int)'

export NAME
NAME=$(wal-g backup-push --config=/root/conf.yaml 2>&1 | grep -oe 'stream_[0-9]*T[0-9]*Z')

mysql_kill_and_clean_data

wal-g backup-fetch "$NAME" --config=/root/conf.yaml

chown -R mysql:mysql "$MYSQLDATA"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql mysql -e 'show tables' | grep testt1
