#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/crypto_test_config.json"
gpg --import /tmp/PGP_KEY
gpg_key_id=`gpg --list-keys | tail -n +4 | head -n 1 | cut -d ' ' -f 7`

COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}

printf ",\n\"WALE_GPG_KEY_ID\":\"${gpg_key_id}\"" >> ${TMP_CONFIG}
tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

WAL_PUSH_LOGS="/tmp/logs/wal_push_logs/pg_crypto_test_logs"
WAL_FETCH_LOGS="/tmp/logs/wal_fetch_logs/pg_crypto_test_logs"
BACKUP_PUSH_LOGS="/tmp/logs/backup_push_logs/pg_crypto_test_logs"
BACKUP_FETCH_LOGS="/tmp/logs/backup_fetch_logs/pg_crypto_test_logs"

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/time -v -a --output ${WAL_PUSH_LOGS} /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 1 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 10 -S &
sleep 1
/usr/bin/time -v -a --output ${BACKUP_PUSH_LOGS} wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
# wal-g will use WALE_GPG_KEY_ID instead of WALG_PGP_KEY_PATH for backup-fetch
unset WALG_PGP_KEY_PATH

tmp/scripts/drop_pg.sh

/usr/bin/time -v -a --output ${BACKUP_FETCH_LOGS} wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/time -v -a --output ${WAL_FETCH_LOGS} /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
echo "Crypto test success!!!!!!"
