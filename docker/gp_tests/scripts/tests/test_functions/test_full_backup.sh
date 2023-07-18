#!/bin/sh
test_full_backup()
{
  TMP_CONFIG=$1
  source /tmp/tests/test_functions/util.sh

  bootstrap_gp_cluster
  sleep 3
  enable_pitr_extension
  setup_wal_archiving

  # 1st backup (init tables heap, ao, co)
  insert_a_lot_of_data
  run_backup_logged ${TMP_CONFIG} ${PGDATA}

  # 2nd backup (populate the co table)
  psql -p 6000 -d test -c "INSERT INTO co select i, i FROM generate_series(1,100000)i;"
  run_backup_logged ${TMP_CONFIG} ${PGDATA}

  # 3rd backup (populate the ao table)
  psql -p 6000 -d test -c "INSERT INTO ao select i, i FROM generate_series(1,100000)i;"
  run_backup_logged ${TMP_CONFIG} ${PGDATA}

  stop_and_delete_cluster_dir

  # show the backup list
  wal-g backup-list --config=${TMP_CONFIG}

  # show the storage objects (useful for debug)
  wal-g st ls -r --config=${TMP_CONFIG}

  wal-g backup-fetch LATEST --in-place --config=${TMP_CONFIG}

  start_cluster

  assert_count 100000 200000 200000
  
  cleanup

  echo "Greenplum backup-push test was successful"
}