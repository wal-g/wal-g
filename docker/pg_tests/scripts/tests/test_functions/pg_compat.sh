#!/bin/sh
# Differences between PostgreSQL 10 and 18, kept in one place instead of in 20+
# tests. POSIX sh only: the tests run under dash.
# NOTE: do not move this file to scripts/scripts/ - that directory is copied
# into the Greenplum and Cloudberry images, which are PG 9.4.

pg_compat_datadir() {
  echo "${1:-$PGDATA}"
}

pg_major() {
  cat "$(pg_compat_datadir "$1")/PG_VERSION"
}

# pg_version_ge <version> [datadir] -> true if the cluster is at least <version>
pg_version_ge() {
  local want="$1"
  local have
  have=$(pg_major "$2")
  if [ -z "$have" ]; then
    echo "pg_compat: cannot read PG_VERSION from $(pg_compat_datadir "$2")" >&2
    exit 1
  fi
  awk -v have="$have" -v want="$want" 'BEGIN { exit !(have + 0 >= want + 0) }'
}

# ---------------------------------------------------------------------------
# Recovery configuration
#
# PG 12 removed recovery.conf and moved its settings to postgresql.conf, with
# recovery.signal or standby.signal to say what is wanted. An old recovery.conf
# is not ignored: the server does not start.
# https://www.postgresql.org/docs/current/recovery-config.html
# ---------------------------------------------------------------------------

# write_recovery_settings [datadir]
# Reads recovery settings from stdin. Use it instead of `> $PGDATA/recovery.conf`:
#
#   echo "restore_command = '...'" | write_recovery_settings
write_recovery_settings() {
  local datadir
  datadir=$(pg_compat_datadir "$1")

  if pg_version_ge 12 "$datadir"; then
    touch "$datadir/recovery.signal"
    cat >> "$datadir/postgresql.conf"
  else
    cat > "$datadir/recovery.conf"
  fi
}

# write_standby_settings [datadir]
# The same, for a streaming standby. standby_mode and trigger_file were removed
# in PG 12, so they are dropped there; standby.signal has that meaning instead.
write_standby_settings() {
  local datadir
  datadir=$(pg_compat_datadir "$1")

  if pg_version_ge 12 "$datadir"; then
    touch "$datadir/standby.signal"
    { grep -vE "^[[:space:]]*(standby_mode|trigger_file)[[:space:]]*=" || true; } \
      >> "$datadir/postgresql.conf"
  else
    cat > "$datadir/recovery.conf"
  fi
}

# setup_recovery_settings <command> [datadir]
# For callers that keep their settings in a function.
setup_recovery_settings() {
  local emit="$1"
  $emit | write_recovery_settings "$2"
}

# ---------------------------------------------------------------------------
# postgresql.conf / pg_hba.conf settings that were renamed
# ---------------------------------------------------------------------------

# wal_keep_conf <segments> [datadir] -> prints the line, the caller adds it.
# PG 13 replaced wal_keep_segments (a number) with wal_keep_size (a size).
wal_keep_conf() {
  local segments="$1"

  if pg_version_ge 13 "$2"; then
    echo "wal_keep_size = $((segments * 16))MB"
  else
    echo "wal_keep_segments = $segments"
  fi
}

# hba_repl_method [datadir] -> the auth method for pg_hba.conf.
# From PG 14 password_encryption is scram-sha-256 by default, so a role with a
# plain password cannot log in through an md5 line.
hba_repl_method() {
  if pg_version_ge 14 "$1"; then
    echo "scram-sha-256"
  else
    echo "md5"
  fi
}

# switch_wal [psql args...] - force a WAL segment switch.
# pg_switch_xlog() was renamed pg_switch_wal() in PG 10.
switch_wal() {
  if pg_version_ge 10; then
    psql "$@" -c 'select pg_switch_wal();'
  else
    psql "$@" -c 'select pg_switch_xlog();'
  fi
}

# ---------------------------------------------------------------------------
# Dump comparison
#
# PG 18 pg_dump adds \restrict and \unrestrict lines with a key that is
# different every time, so remove them before comparing.
# ---------------------------------------------------------------------------

pg_compat_strip_dump() {
  sed -i '/^\\restrict /d; /^\\unrestrict /d' "$@"
}

# dump_all <outfile> [pg_dumpall args...]
dump_all() {
  local out="$1"
  shift
  pg_dumpall -f "$out" "$@"
  pg_compat_strip_dump "$out"
}

# dump_db <outfile> [pg_dump args...]
dump_db() {
  local out="$1"
  shift
  pg_dump -f "$out" "$@"
  pg_compat_strip_dump "$out"
}

compare_dumps() {
  diff "$1" "$2"
}

# ---------------------------------------------------------------------------
# Which versions a test applies to
#
# These checks read PG_MAJOR from the image, so they work as the first line of a
# test. Exit code 77 means "skipped".
# ---------------------------------------------------------------------------

# require_pg_ge <version> - the tested feature was added in <version>.
# There is no upper limit, so a new release needs no change here.
require_pg_ge() {
  if [ "${PG_MAJOR}" -lt "$1" ]; then
    echo "SKIP: needs PostgreSQL >= $1, this image is ${PG_MAJOR}"
    exit 77
  fi
}

# require_pg_le <version> - the tested thing will never support newer versions
# (wal-e, for example).
require_pg_le() {
  if [ "${PG_MAJOR}" -gt "$1" ]; then
    echo "SKIP: only applies to PostgreSQL <= $1, this image is ${PG_MAJOR}"
    exit 77
  fi
}

# skip_pg <version> - skip this test on one version.
skip_pg() {
  if [ "${PG_MAJOR}" = "$1" ]; then
    echo "SKIP: known broken on PostgreSQL $1"
    exit 77
  fi
}
