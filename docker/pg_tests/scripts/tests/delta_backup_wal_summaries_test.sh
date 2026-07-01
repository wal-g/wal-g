#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
prepare_config "/tmp/configs/delta_backup_wal_summaries_test_config.json"

initdb ${PGDATA}

cat >> ${PGDATA}/postgresql.conf <<EOF
archive_mode = on
archive_command = '/usr/bin/timeout 600 /usr/bin/wal-g --config=${TMP_CONFIG} wal-push %p'
archive_timeout = 600
summarize_wal = on
EOF

pg_ctl -D ${PGDATA} -w start

wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

pgbench -i -s 4 postgres
wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

pgbench -i -s 8 postgres
pg_dumpall -f /tmp/dump1

# Force a checkpoint + segment switch so the walsummarizer has a clean LSN to flush a summary at,
# then wait for summarized_lsn to reach the post-switch flush LSN before requesting the increment.
psql -c "CHECKPOINT;" postgres
psql -c "SELECT pg_switch_wal();" postgres
target=$(psql -tAc "SELECT pg_current_wal_flush_lsn();" postgres)
for _ in $(seq 1 60); do
  ok=$(psql -tAc "SELECT summarized_lsn >= '${target}'::pg_lsn FROM pg_get_wal_summarizer_state();" postgres)
  [ "$ok" = "t" ] && break
  sleep 1
done

wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --delta-from-wal-summaries

# Sanity: latest backup must be a delta (suffix _D_<base>).
latest=$(wal-g --config=${TMP_CONFIG} backup-list | tail -n 1 | cut -f 1 -d " ")
case "${latest}" in
  *_D_*) ;;
  *) echo "expected delta backup, got ${latest}" >&2; exit 1 ;;
esac

/tmp/scripts/drop_pg.sh

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

touch ${PGDATA}/recovery.signal
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& /usr/bin/wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

# PG18 pg_dumpall emits \restrict/\unrestrict with per-invocation random keys
sed -i '/^\\restrict /d; /^\\unrestrict /d' /tmp/dump1 /tmp/dump2
diff /tmp/dump1 /tmp/dump2

psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres
wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm
/tmp/scripts/drop_pg.sh
rm ${TMP_CONFIG}
