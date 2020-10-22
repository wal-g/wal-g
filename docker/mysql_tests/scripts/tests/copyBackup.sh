#!/bin/sh
set -e -x

. /usr/local/export_common.sh

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start
mysql mysql -e 'create table testt1(i int)'


cat > /root/from.yaml <<EOH
WALE_S3_PREFIX: s3://mysqlcopybackupfrom
WALG_S3_MAX_PART_SIZE: 5242880
EOH

cat > /root/to.yaml <<EOH
WALE_S3_PREFIX: s3://mysqlcopybackupto
WALG_S3_MAX_PART_SIZE: 5242880
EOH

wal-g backup-list --config=/root/from.yaml
wal-g delete everything FORCE --confirm --config=/root/from.yaml
wal-g backup-list --config=/root/from.yaml

export name=$(wal-g backup-push --config=/root/from.yaml 2>&1 | grep -oe 'stream_[0-9]*T[0-9]*Z')
sleep 1

echo $name

wal-g backup-list --config=/root/from.yaml
wal-g backup-list --config=/root/to.yaml

wal-g copy --from=/root/from.yaml --to=/root/to.yaml --backup "$name" --config=/root/from.yaml

mysql_kill_and_clean_data

wal-g backup-fetch "$name" --config=/root/to.yaml

chown -R mysql:mysql $MYSQLDATA
service mysql start || (cat /var/log/mysql/error.log && false)
mysql mysql -e 'show tables' | grep testt1
