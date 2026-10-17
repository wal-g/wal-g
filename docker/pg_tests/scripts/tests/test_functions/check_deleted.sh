#!/bin/sh

# backup_names LIST_FILE - print backup names (1st column) from a saved backup-list output, skipping the header
backup_names() {
  tail -n +2 "$1" | cut -f 1 -d " "
}

# new_backup_names OLD_LIST_FILE NEW_LIST_FILE - print names that are present in NEW_LIST_FILE but not in OLD_LIST_FILE
new_backup_names() {
  backup_names "$1" > /tmp/old_backup_names
  backup_names "$2" | sort -u | grep -vxF -f /tmp/old_backup_names || true
}

# assert_backups_deleted NAME... - fail if any of the given backups is still listed by backup-list
# or still has any object in the storage(s). Set CHECK_STORAGES (e.g. "default good_failover") to check
# several storages explicitly; otherwise only the default one is checked.
assert_backups_deleted() {
  if [ "$#" -eq 0 ]; then
    echo "assert_backups_deleted: no backup names given, nothing to check"
    exit 1
  fi
  current_list=$(wal-g --config=${TMP_CONFIG} backup-list 2>/dev/null || true)
  for name in "$@"; do
    if echo "${current_list}" | grep -qF "${name}"; then
      echo "backup ${name} is expected to be deleted but is still in backup-list"
      exit 1
    fi
    if [ -z "${CHECK_STORAGES}" ]; then
      storage_objects=$(wal-g --config=${TMP_CONFIG} st ls -r basebackups_005/ 2>/dev/null || true)
      if echo "${storage_objects}" | grep -qF "${name}"; then
        echo "backup ${name} is expected to be deleted but still has objects in the storage"
        exit 1
      fi
    else
      for storage in ${CHECK_STORAGES}; do
        storage_objects=$(wal-g --config=${TMP_CONFIG} st ls -r basebackups_005/ --target "${storage}" 2>/dev/null || true)
        if echo "${storage_objects}" | grep -qF "${name}"; then
          echo "backup ${name} is expected to be deleted but still has objects in storage ${storage}"
          exit 1
        fi
      done
    fi
  done
}
