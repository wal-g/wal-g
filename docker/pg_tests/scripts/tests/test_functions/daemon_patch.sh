#!/bin/sh
set -e

recovery_conf() {
  echo "recovery_target_action = promote"
  echo "restore_command = '/tmp/tests/external_commands/daemon_nc_send_wal_fetch.sh %f %p'"
}

archive_conf() {
  echo "archive_command = '/tmp/tests/external_commands/daemon_nc_send_wal_push.sh %f'"
  echo "archive_mode = on"
  echo "logging_collector=on"
  echo "wal_level=replica"
  echo "max_wal_senders=5"
}

start_daemon() {
  echo "Start wal-g daemon"

  if [ -z "${TMP_CONFIG}" ]; then
    echo "env TMP_CONFIG isn't set"
    exit 1
  fi

  if [ -z "${WALG_SOCKET}" ]; then
    echo "env WALG_SOCKET isn't set"
    exit 1
  fi

  wal-g --config="${TMP_CONFIG}" daemon "${WALG_SOCKET}" &
  until [ -S "${WALG_SOCKET}" ]
  do
    sleep 1
  done
  echo "daemon started"
}

drop_pg() {
  pkill -9 postgres || true
  rm -rf $PGDATA /tmp/basebackups_005 /tmp/wal_005 /tmp/spaces /tmp/spaces_backup
}
