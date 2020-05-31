#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mariadbfullmysqldumpbucket
export WALG_STREAM_CREATE_COMMAND="mysqldump --all-databases --single-transaction"
export WALG_STREAM_RESTORE_COMMAND="mysql"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND=


mysql_install_db > /dev/null
service mysql start

sysbench --table-size=10 prepare

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push


ps aux | grep mysqld_safe

mariadb_kill_and_clean_data

mysql_install_db > /dev/null
service mysql start

wal-g backup-fetch LATEST

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_backup /tmp/dump_after_restore
