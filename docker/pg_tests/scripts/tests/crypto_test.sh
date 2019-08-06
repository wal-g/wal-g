#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/crypto_test_config.json"
gpg --import /tmp/PGP_KEY
gpg_key_id=`gpg --list-keys | tail -n +4 | head -n 1 | cut -d ' ' -f 7`

COMMON_CONFIG="/tmp/configs/common_config.json"

echo "," >> ${CONFIG_FILE}
cat ${COMMON_CONFIG} >> ${CONFIG_FILE}


printf ",\n\"WALE_GPG_KEY_ID\":\"${gpg_key_id}\"" >> ${CONFIG_FILE}
tmp/scripts/wrap_config_file.sh ${CONFIG_FILE}
cat ${CONFIG_FILE}

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${CONFIG_FILE} wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pgbench -i -s 1 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 10 -S &
sleep 1
wal-g --config=${CONFIG_FILE} backup-push ${PGDATA}
# wal-g will use WALE_GPG_KEY_ID instead of WALG_PGP_KEY_PATH for backup-fetch
unset WALG_PGP_KEY_PATH

tmp/scripts/drop_pg.sh

wal-g --config=${CONFIG_FILE} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${CONFIG_FILE} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2

tmp/scripts/drop_pg.sh

echo "Crypto test success!!!!!!"
