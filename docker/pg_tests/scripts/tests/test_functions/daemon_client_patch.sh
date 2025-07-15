#!/bin/sh
set -e

recovery_conf() {
  echo "recovery_target_action = promote"
  echo "restore_command = 'walg-daemon-client ${WALG_SOCKET} wal-fetch %f %p'"
}

archive_conf() {
  echo "archive_command = 'walg-daemon-client ${WALG_SOCKET} wal-push %f'"
  echo "archive_mode = on"
  echo "logging_collector=on"
  echo "wal_level=replica"
  echo "max_wal_senders=5"
}
