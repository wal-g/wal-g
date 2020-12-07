#!/bin/sh
set -e -x

. /usr/local/export_common.sh

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
mysql mysql -e 'create table testt1(i int)'

cat > /root/from.yaml <<EOH
WALE_S3_PREFIX: "s3://mysqlcopybackupfrom"
AWS_ENDPOINT: "http://s3:9000"
AWS_ACCESS_KEY_ID: "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
WALG_S3_MAX_PART_SIZE: 5242880
EOH

cat > /root/to.yaml <<EOH
WALE_S3_PREFIX: "s3://mysqlcopybackupto"
AWS_ENDPOINT: "http://s3:9000"
AWS_ACCESS_KEY_ID: "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
WALG_S3_MAX_PART_SIZE: 5242880
EOH

wal-g delete everything FORCE --confirm --config=/root/from.yaml
wal-g delete everything FORCE --confirm --config=/root/to.yaml

export NAME
NAME=$(wal-g backup-push --config=/root/from.yaml 2>&1 | grep -oe 'stream_[0-9]*T[0-9]*Z')

sleep 1

echo "$NAME"

wal-g backup-list --config=/root/from.yaml
wal-g backup-list --config=/root/to.yaml

wal-g backup-copy --from=/root/from.yaml --to=/root/to.yaml --backup "$NAME"

mysql_kill_and_clean_data

wal-g backup-fetch "$NAME" --config=/root/to.yaml

chown -R mysql:mysql "$MYSQLDATA"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql mysql -e 'show tables' | grep testt1
