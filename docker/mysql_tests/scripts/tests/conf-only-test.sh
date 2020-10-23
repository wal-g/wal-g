#!/bin/sh
set -e -x

. /usr/local/export_test_funcs.sh

cat > /root/conf.yaml <<EOH
WALE_S3_PREFIX: s3://mysqlcopybackupfrom
AWS_ENDPOINT: http://s3:9000"
AWS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
WALG_S3_MAX_PART_SIZE: 5242880
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
