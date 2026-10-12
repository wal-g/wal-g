#!/bin/sh
set -e -x

. /tmp/tests/test_functions/pg_compat.sh
. /tmp/tests/test_functions/prepare_config.sh
CONFIG_FILE="/tmp/configs/crypto_test_config.json"
gpg --import /tmp/PGP_KEY
gpg_key_id=`gpg --list-keys | tail -n +4 | head -n 1 | cut -d ' ' -f 7`

TMP_CONFIG="/tmp/configs/tmp_config.json"
prepare_config "${CONFIG_FILE}"
jq --arg gpg_key_id "${gpg_key_id}" '. + {"WALE_GPG_KEY_ID": $gpg_key_id}' \
    "${TMP_CONFIG}" > "${TMP_CONFIG}.new"
mv "${TMP_CONFIG}.new" "${TMP_CONFIG}"

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 1 postgres
dump_all /tmp/dump1
pgbench -c 2 -T 10 -S &
sleep 1
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}
# wal-g will use WALE_GPG_KEY_ID instead of WALG_PGP_KEY_PATH for backup-fetch
unset WALG_PGP_KEY_PATH
/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" | write_recovery_settings

pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
dump_all /tmp/dump2

compare_dumps /tmp/dump1 /tmp/dump2
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
