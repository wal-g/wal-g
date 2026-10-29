#!/bin/sh
set -e -x

. /tmp/tests/test_functions/prepare_config.sh
. /tmp/tests/test_functions/pg_compat.sh
prepare_config /tmp/configs/backup_fetch_wal_test_config.json

work=$(mktemp -d)
backup_pid=
cleanup() {
  if [ -n "$backup_pid" ]; then
    kill "$backup_pid" 2>/dev/null || true
    wait "$backup_pid" 2>/dev/null || true
  fi
  pg_ctl -D "$work/restored" -m immediate -w stop 2>/dev/null || true
  pg_ctl -D "$PGDATA" -m immediate -w stop 2>/dev/null || true
  rm -rf "$work"
  rm -f "$TMP_CONFIG"
}
trap cleanup EXIT

initdb -D "$PGDATA"
{
  echo "archive_mode = on"
  echo "archive_command = 'wal-g --config=$TMP_CONFIG wal-push %p'"
  wal_keep_conf 64
} >> "$PGDATA/postgresql.conf"
pg_ctl -D "$PGDATA" -w start

# Slow the data-file copy enough to switch WAL repeatedly during each backup.
# Update this relation before the delta so it also has data files to copy.
psql -v ON_ERROR_STOP=1 -c "CREATE TABLE fetch_wal_probe AS SELECT i, repeat('a', 1000) AS payload FROM generate_series(1, 16000) i"

for kind in full delta; do
  if [ "$kind" = delta ]; then
    psql -v ON_ERROR_STOP=1 -c "UPDATE fetch_wal_probe SET payload = repeat('b', 1000)"
  fi
  dump_db "$work/$kind.sql" postgres
  WALG_DISK_RATE_LIMIT=4194304 wal-g --config="$TMP_CONFIG" backup-push "$PGDATA" > "$work/backup.log" 2>&1 &
  backup_pid=$!
  # No workload after the backup: the fetched WAL must suffice on its own.
  # Bound the producer even if backup-push gets stuck; wait propagates failures.
  for attempt in $(seq 1 60); do
    if ! kill -0 "$backup_pid" 2>/dev/null; then break; fi
    psql -v ON_ERROR_STOP=1 -c "SELECT pg_create_restore_point('fetch_wal_test'), pg_switch_wal()"
    sleep 1
  done
  wait "$backup_pid" || { cat "$work/backup.log"; exit 1; }
  backup_pid=

  backup=$(wal-g --config="$TMP_CONFIG" backup-list --json | jq -er 'sort_by(.start_time) | last | .backup_name')
  wal-g --config="$TMP_CONFIG" st cat "basebackups_005/${backup}_backup_stop_sentinel.json" > "$work/sentinel.json"
  if [ "$kind" = delta ]; then
    jq -e '.DeltaFrom != null' "$work/sentinel.json"
  else
    jq -e '.DeltaFrom == null' "$work/sentinel.json"
  fi

  for reverse in false true; do
    restored="$work/restored"
    wal-g --config="$TMP_CONFIG" backup-fetch "$restored" "$backup" --fetch-wal --reverse-unpack="$reverse"

    # Derive the start independently from the restored label, and the exclusive
    # end from the sentinel. Do not use StartWalFileName: at a segment boundary
    # it may refer to the preceding segment. These clusters use 16 MiB segments.
    first=$(sed -n 's/^START WAL LOCATION:.*(file \([0-9A-F]*\)).*/\1/p' "$restored/backup_label")
    test "${#first}" -eq 24
    timeline=$(printf '%s' "$first" | cut -c 1-8)
    log=$(printf '%s' "$first" | cut -c 9-16)
    seg=$(printf '%s' "$first" | cut -c 17-24)
    first_no=$((0x$log * 256 + 0x$seg))
    last_no=$(jq -er '((.FinishLSN - 1) / 16777216) | floor' "$work/sentinel.json")
    test "$((last_no - first_no))" -ge 2
    for number in $(seq "$first_no" "$last_no"); do
      name=$(printf '%s%08X%08X' "$timeline" "$((number / 256))" "$((number % 256))")
      # Compare every byte before PostgreSQL can recycle or modify these files.
      cmp "$PGDATA/pg_wal/$name" "$restored/pg_wal/$name"
    done

    # The primary is still using its own port. Recovery gets a separate socket
    # directory and port, and cannot obtain missing WAL from any external source.
    {
      echo "port = 5433"
      echo "unix_socket_directories = '$work'"
      echo "archive_mode = off"
    } >> "$restored/postgresql.conf"
    {
      echo "restore_command = 'false'"
      echo "recovery_target = 'immediate'"
      echo "recovery_target_action = 'promote'"
    } | write_recovery_settings "$restored"
    pg_ctl -D "$restored" -l "$work/recovery.log" -w -t 60 start || { cat "$work/recovery.log"; exit 1; }
    # pg_ctl can return when hot standby accepts connections, before promotion
    # finishes. Wait for recovery to end before checking the restored data.
    for attempt in $(seq 1 60); do
      in_recovery=$(PGHOST="$work" PGPORT=5433 psql -At -v ON_ERROR_STOP=1 -c 'SELECT pg_is_in_recovery()') || { cat "$work/recovery.log"; exit 1; }
      if [ "$in_recovery" = f ]; then break; fi
      sleep 1
    done
    test "$in_recovery" = f || { cat "$work/recovery.log"; exit 1; }
    PGHOST="$work" PGPORT=5433 dump_db "$work/restored.sql" postgres
    compare_dumps "$work/$kind.sql" "$work/restored.sql"
    pg_ctl -D "$restored" -m fast -w stop
    rm -rf "$restored"
  done
done

pg_ctl -D "$PGDATA" -m fast -w stop
rm -rf "$PGDATA"
