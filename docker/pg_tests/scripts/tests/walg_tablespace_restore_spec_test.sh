#!/bin/sh
# Test for WAL-G to WAL-G tablespace spec compatibility with --restore-spec flag
# This test verifies that:
# 1. WAL-G can create backups with tablespaces
# 2. WAL-G can extract the tablespace spec from its own backup
# 3. WAL-G can restore using --restore-spec to different tablespace locations
set -e -x

initdb ${PGDATA}

echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
echo "archive_command = '\
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=brotli \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
/usr/bin/timeout 600 wal-g wal-push %p'" >> ${PGDATA}/postgresql.conf
echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

# Create tablespaces in original locations
mkdir -p /tmp/spaces/space
mkdir -p /tmp/spaces/space2
psql -c "create tablespace space location '/tmp/spaces/space';"
psql -c "create table cinemas (id integer, name text, location text) tablespace space;"
psql -c "insert into cinemas (id, name, location) values (1, 'Inception', 'USA');"
psql -c "insert into cinemas (id, name, location) values (2, 'Taxi', 'France');"
psql -c "insert into cinemas (id, name, location) values (3, 'Spirited Away', 'Japan');"
psql -c "create tablespace space2 location '/tmp/spaces/space2';"
psql -c "create table series (id integer, name text) tablespace space2;"
psql -c "insert into series (id, name) values (1, 'Game of Thrones');"
psql -c "insert into series (id, name) values (2, 'Black mirror');"
psql -c "insert into series (id, name) values (3, 'Sherlock');"
psql -c "create table users (id integer, name text, password text);"
psql -c "insert into users (id, name, password) values(1, 'ismirn0ff', 'password');"
psql -c "insert into users (id, name, password) values(2, 'tinsane', 'qwerty');"
psql -c "insert into users (id, name, password) values(3, 'godjan', 'g0djan');"
psql -c "insert into users (id, name, password) values(4, 'x4m', 'borodin');"
pg_dumpall -f /tmp/dump1
sleep 1

# Create WAL-G backup with tablespaces
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=brotli \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
wal-g backup-push ${PGDATA}

pkill -9 postgres

# Find WAL-G backup sentinel and extract tablespace spec
# NOTE: WAL-G outputs "Spec" (uppercase) for backward compatibility with existing scripts
# But it can read both "Spec" (WAL-G) and "spec" (WAL-E) when restoring
cd /tmp/basebackups_005
BACKUP_SENTINEL=$(ls | grep _backup_stop_sentinel.json | head -1)

echo "=== Original WAL-G Sentinel ==="
cat "$BACKUP_SENTINEL"

# Extract spec using uppercase .Spec (WAL-G format)
echo "=== Extracting tablespace spec from WAL-G backup ==="
cat "$BACKUP_SENTINEL" | jq .Spec > /tmp/original_restore_spec.json
echo "Extracted spec:"
cat /tmp/original_restore_spec.json

# Create a modified spec that uses different tablespace locations
# This simulates restoring to a new machine with different paths
mkdir -p /tmp/new_spaces/new_space
mkdir -p /tmp/new_spaces/new_space2

# Get tablespace OIDs from the original spec
TBLSPC1_OID=$(cat /tmp/original_restore_spec.json | jq -r '.tablespaces[0]')
TBLSPC2_OID=$(cat /tmp/original_restore_spec.json | jq -r '.tablespaces[1]')

# Create modified restore spec with new locations
cat > /tmp/modified_restore_spec.json << EOF
{
  "base_prefix": "${PGDATA}",
  "tablespaces": ["${TBLSPC1_OID}", "${TBLSPC2_OID}"],
  "${TBLSPC1_OID}": { "loc": "/tmp/new_spaces/new_space", "link": "pg_tblspc/${TBLSPC1_OID}" },
  "${TBLSPC2_OID}": { "loc": "/tmp/new_spaces/new_space2", "link": "pg_tblspc/${TBLSPC2_OID}" }
}
EOF

echo "=== Modified restore spec with new tablespace locations ==="
cat /tmp/modified_restore_spec.json

# Save config files
mkdir /tmp/conf_files
cp -t /tmp/conf_files/ ${PGDATA}/postgresql.conf ${PGDATA}/pg_hba.conf ${PGDATA}/pg_ident.conf

# Clean up for restore
rm -rf /tmp/spaces/*
rm -rf /tmp/new_spaces/*
rm -rf ${PGDATA}

# Restore using --restore-spec with modified tablespace locations
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=brotli \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
wal-g backup-fetch --restore-spec /tmp/modified_restore_spec.json ${PGDATA} LATEST

# Verify tablespace symlinks point to new locations
echo "=== Verifying tablespace symlinks ==="
ls -la ${PGDATA}/pg_tblspc/

# TODO: i don't know what to do with tablespace_map in walg
cat ${PGDATA}/tablespace_map
rm ${PGDATA}/tablespace_map

# Setup recovery
echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& \
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=brotli \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
/usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

cp -t ${PGDATA} /tmp/conf_files/postgresql.conf /tmp/conf_files/pg_hba.conf /tmp/conf_files/pg_ident.conf

pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

# Verify data is identical
sed -i "s|LOCATION '/tmp/spaces/space'|LOCATION '/tmp/new_spaces/new_space'|" /tmp/dump1
sed -i "s|LOCATION '/tmp/spaces/space2'|LOCATION '/tmp/new_spaces/new_space2'|" /tmp/dump1
diff /tmp/dump1 /tmp/dump2

# Verify tablespace files are in the NEW locations
echo "=== Verifying tablespace data is in new locations ==="
ls -la /tmp/new_spaces/new_space/
ls -la /tmp/new_spaces/new_space2/

# Verify OLD locations are still empty
if [ -z "$(ls -A /tmp/spaces/space 2>/dev/null)" ]; then
    echo "SUCCESS: Old tablespace location /tmp/spaces/space is empty"
else
    echo "FAIL: Old tablespace location still has data"
    exit 1
fi

/tmp/scripts/drop_pg.sh
rm -rf /tmp/conf_files

echo "=== TEST PASSED: WAL-G tablespace restore-spec works correctly ==="

