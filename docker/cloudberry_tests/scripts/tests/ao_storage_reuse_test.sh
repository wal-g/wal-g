#!/bin/bash
set -e -x -o pipefail

# Compaction can reuse an AO segfile with different contents but the same EOF and modcount.
# The incremental backup must notice the changed mtime and upload the file again.
CONFIG_FILE="/tmp/configs/delta_backup_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat "${CONFIG_FILE}" > "${TMP_CONFIG}"
echo "," >> "${TMP_CONFIG}"
cat "${COMMON_CONFIG}" >> "${TMP_CONFIG}"
/tmp/pg_scripts/wrap_config_file.sh "${TMP_CONFIG}"
source /tmp/tests/test_functions/util.sh

TEST_DIR=$(mktemp -d)
trap 'rm -rf "${TEST_DIR}"' EXIT

bootstrap_gp_cluster
setup_wal_archiving
wal-g --config="${TMP_CONFIG}" delete everything FORCE --confirm

psql -X -v ON_ERROR_STOP=1 -p 7000 -c "CREATE DATABASE test"
psql -X -v ON_ERROR_STOP=1 -p 7000 -d test <<'SQL'
CREATE TABLE public.ao_reuse_demo (k integer, id integer)
WITH (appendonly=true, orientation=row, compresstype=none, blocksize=32768)
DISTRIBUTED BY (k);

-- All rows go to one segment. The first segfile gets IDs 1..100 and 201,
-- the second gets IDs 101..200.
SET gp_appendonly_insert_files = 2;
SET gp_appendonly_insert_files_tuples_range = 100;
INSERT INTO public.ao_reuse_demo SELECT 1, i FROM generate_series(1, 201) AS i ORDER BY i;
SET gp_appendonly_insert_files = 0;

-- Expected __gp_aoseg output (segment_id = 1 in this example):
--  segment_id | segno | tupcount | eof  | modcount | state
-- ------------+-------+----------+------+----------+-------
--           1 |     1 |      101 | 1856 |        1 |     1
--           1 |     2 |      100 | 1832 |        1 |     1

-- Leave 100 live rows in the first segfile and 50 in the second.
-- Only the second segfile exceeds the 10% compaction threshold.
DELETE FROM public.ao_reuse_demo WHERE id BETWEEN 101 AND 150 OR id = 201;
-- Expected __gp_aoseg output at the base backup: tupcount still includes deleted rows.
--  segment_id | segno | tupcount | eof  | modcount | state
-- ------------+-------+----------+------+----------+-------
--           1 |     1 |      101 | 1856 |        1 |     1
--           1 |     2 |      100 | 1832 |        2 |     1
SQL

# Save the logical contents and take the base backup before compaction changes the files.
psql -X -v ON_ERROR_STOP=1 -p 7000 -d test -At \
    -c "SELECT k, id FROM public.ao_reuse_demo ORDER BY id" > "${TEST_DIR}/expected_rows"
# The common config sets the AO size threshold to zero, so the small segfiles use AO storage.
run_backup_logged "${TMP_CONFIG}" "${PGDATA}" "--full"

# Remember the second segfile and verify that BOTH EOF and modcount survive its reuse.
# ON_ERROR_STOP and pipefail also make SQL errors fail the test.
psql -X -v ON_ERROR_STOP=1 -p 7000 -d test -At <<'SQL' | grep -x 't|t'
SELECT segment_id AS target_content, segno AS target_segno,
       eof AS old_eof, modcount AS old_modcount
FROM gp_toolkit.__gp_aoseg('public.ao_reuse_demo')
WHERE tupcount = 100
\gset

SET gp_appendonly_compaction = on;
SET gp_appendonly_compaction_threshold = 10;
-- Move the second segfile's 50 live rows to a new file and empty the second segfile.
VACUUM public.ao_reuse_demo;
-- Expected __gp_aoseg output: segno 2 is empty, but retains modcount = 2.
--  segment_id | segno | tupcount | eof  | modcount | state
-- ------------+-------+----------+------+----------+-------
--           1 |     1 |      101 | 1856 |        1 |     1
--           1 |     2 |        0 |    0 |        2 |     1
--           1 |     3 |       50 |  936 |        0 |     1

-- Move the first segfile's 100 live rows into the emptied second segfile.
VACUUM FULL public.ao_reuse_demo;
-- Expected output for the target segfile, compared with the base backup:
--  segment_id | segno | tupcount | eof  | modcount | state | same_eof | same_modcount
-- ------------+-------+----------+------+----------+-------+----------+---------------
--           1 |     2 |      100 | 1832 |        2 |     1 | t        | t
-- Segno 2 used to hold IDs 101..200; now it holds IDs 1..100.
-- EOF and modcount alone would incorrectly identify the recycled file as unchanged.

-- The reused segfile contains different rows, but its EOF and modcount are unchanged.
SELECT eof = :old_eof AS same_eof, modcount = :old_modcount AS same_modcount
FROM gp_toolkit.__gp_aoseg('public.ao_reuse_demo')
WHERE segment_id = :target_content AND segno = :target_segno;
SQL

# Back up the reused file, then restore the incremental backup into empty data directories.
run_backup_logged "${TMP_CONFIG}" "${PGDATA}" ""
stop_and_delete_cluster_dir
wal-g --config="${TMP_CONFIG}" backup-fetch LATEST --in-place
start_cluster

# Compare every row: the old and new segfile have the same row count, but different IDs.
psql -X -v ON_ERROR_STOP=1 -p 7000 -d test -At \
    -c "SELECT k, id FROM public.ao_reuse_demo ORDER BY id" > "${TEST_DIR}/restored_rows"
diff -u "${TEST_DIR}/expected_rows" "${TEST_DIR}/restored_rows"

cleanup
rm "${TMP_CONFIG}"
