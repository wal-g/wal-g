# PostgreSQL Snapshot Backups

## Overview

WAL-G now supports **snapshot backups** for PostgreSQL databases. This feature allows you to create backups using filesystem-level or cloud disk snapshots (e.g., AWS EBS, Azure Managed Disks, Google Persistent Disks, or ZFS/LVM snapshots) while maintaining proper database consistency and integration with WAL-G's point-in-time recovery (PITR) capabilities.

## Key Concepts

### What are Snapshot Backups?

Snapshot backups leverage storage-level snapshot capabilities to create instant backups of your PostgreSQL data directory. WAL-G:

1. Calls `pg_start_backup()` to ensure database consistency
2. Executes your custom snapshot command (e.g., creating an EBS snapshot)
3. Calls `pg_stop_backup()` to complete the backup
4. Uploads metadata to WAL-G storage for tracking and PITR

The actual database files remain in the snapshot and are **not uploaded** to WAL-G storage. WAL-G only stores:
- Backup metadata (LSN positions, timestamp, etc.)
- WAL files for point-in-time recovery

### Benefits

- **Speed**: Snapshot creation is nearly instantaneous regardless of database size
- **Cost-effective**: No need to upload large database files to object storage
- **Native Integration**: Leverage your cloud provider's snapshot features
- **PITR Support**: Full point-in-time recovery using WAL archives
- **Minimal I/O Impact**: No file-level copying during backup

## Configuration

### Required Settings

#### `WALG_SNAPSHOT_COMMAND`

The command WAL-G will execute to create the snapshot. This is **required** for `snapshot-push`.

**Environment Variables Available to the Command:**
- `WALG_SNAPSHOT_NAME`: The backup name (timestamp-based)
- `WALG_PG_DATA`: The PostgreSQL data directory path
- `WALG_SNAPSHOT_START_LSN`: The LSN when backup started
- `WALG_SNAPSHOT_START_WAL_FILE`: The WAL file name at backup start (useful for tagging/identifying)

**Example for AWS EBS:**
```bash
export WALG_SNAPSHOT_COMMAND='aws ec2 create-snapshot \
  --volume-id vol-1234567890abcdef0 \
  --description "WAL-G Snapshot: $WALG_SNAPSHOT_NAME" \
  --tag-specifications "ResourceType=snapshot,Tags=[{Key=WalgBackupName,Value=$WALG_SNAPSHOT_NAME},{Key=WalgStartLSN,Value=$WALG_SNAPSHOT_START_LSN},{Key=WalgStartWAL,Value=$WALG_SNAPSHOT_START_WAL_FILE}]"'
```

**Example for Azure:**
```bash
export WALG_SNAPSHOT_COMMAND='az snapshot create \
  --resource-group myResourceGroup \
  --name "walg-$WALG_SNAPSHOT_NAME" \
  --source /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Compute/disks/{disk-name} \
  --tags BackupName=$WALG_SNAPSHOT_NAME StartLSN=$WALG_SNAPSHOT_START_LSN StartWAL=$WALG_SNAPSHOT_START_WAL_FILE'
```

**Example for Google Cloud:**
```bash
export WALG_SNAPSHOT_COMMAND='gcloud compute disks snapshot postgres-disk \
  --snapshot-names="walg-$WALG_SNAPSHOT_NAME" \
  --labels=walg-backup-name=$(echo $WALG_SNAPSHOT_NAME | tr "_" "-"),walg-start-lsn=$WALG_SNAPSHOT_START_LSN \
  --description="WAL-G Snapshot: $WALG_SNAPSHOT_NAME, WAL: $WALG_SNAPSHOT_START_WAL_FILE"'
```

**Example for ZFS:**
```bash
export WALG_SNAPSHOT_COMMAND='zfs snapshot tank/postgres@walg-$WALG_SNAPSHOT_NAME'
```

**Example for LVM:**
```bash
export WALG_SNAPSHOT_COMMAND='lvcreate --size 10G --snapshot --name walg-$WALG_SNAPSHOT_NAME /dev/vg0/postgres-lv'
```

#### `WALG_SNAPSHOT_DELETE_COMMAND` (Optional)

The command WAL-G will execute when a snapshot backup is deleted. This is **optional** but recommended for automatic cleanup.

**Environment Variables Available:**
- `WALG_SNAPSHOT_NAME`: The backup name being deleted

**Example for AWS EBS:**
```bash
export WALG_SNAPSHOT_DELETE_COMMAND='SNAPSHOT_ID=$(aws ec2 describe-snapshots \
  --filters "Name=tag:WalgBackupName,Values=$WALG_SNAPSHOT_NAME" \
  --query "Snapshots[0].SnapshotId" --output text) && \
  aws ec2 delete-snapshot --snapshot-id $SNAPSHOT_ID'
```

**Example for Azure:**
```bash
export WALG_SNAPSHOT_DELETE_COMMAND='az snapshot delete \
  --resource-group myResourceGroup \
  --name "walg-$WALG_SNAPSHOT_NAME"'
```

**Example for Google Cloud:**
```bash
export WALG_SNAPSHOT_DELETE_COMMAND='gcloud compute snapshots delete "walg-$WALG_SNAPSHOT_NAME" --quiet'
```

**Example for ZFS:**
```bash
export WALG_SNAPSHOT_DELETE_COMMAND='zfs destroy tank/postgres@walg-$WALG_SNAPSHOT_NAME'
```

### Standard WAL-G Settings

All standard WAL-G settings still apply:
- `WALG_STORAGE_PREFIX`: Where metadata and WAL files are stored
- `PGHOST`, `PGPORT`, `PGUSER`, etc.: PostgreSQL connection settings
- WAL archiving configuration (for PITR)

## Usage

### Creating a Snapshot Backup

```bash
# Automatically detects data directory from PostgreSQL connection
wal-g snapshot-push

# Or specify data directory explicitly
wal-g snapshot-push /var/lib/postgresql/data
```

**Notes:**
- The data directory is **optional** - WAL-G will automatically query PostgreSQL for it if not provided
- Can be run on **standby servers** (read replicas) without issues
- The actual database data is **not uploaded** to WAL-G storage (only small metadata files are uploaded)
- Although snapshot backups don't compress data, you still need to set `WALG_COMPRESSION_METHOD` to a valid value (e.g., `lz4`, `lzma`, `zstd`) for WAL-G's uploader initialization

**Flags:**
- `--permanent` or `-p`: Mark the backup as permanent (won't be deleted by retention policies)
- `--add-user-data <json>`: Add custom metadata to the backup
- `--target-storage <name>`: Specify which storage to use (for multi-storage configurations)

**Examples:**
```bash
# Simple snapshot backup (auto-detects data directory)
wal-g snapshot-push

# Permanent snapshot backup with metadata
wal-g snapshot-push --permanent \
  --add-user-data '{"description":"pre-upgrade-backup","ticket":"JIRA-123"}'

# On a standby server
wal-g snapshot-push

# With explicit data directory
wal-g snapshot-push /var/lib/postgresql/data
```

### Listing Backups

Snapshot backups appear in the normal backup list:

```bash
wal-g backup-list
```

Snapshot backups are identified by:
- `FilesMetadataDisabled: true`
- `CompressedSize: 0`
- `UncompressedSize: 0`

### Deleting Snapshot Backups

> **⚠️ Note**: Snapshot backup deletion is currently not fully implemented. The `delete` command will fail when trying to delete snapshot backups. This is a known limitation that will be addressed in a future release.

Snapshot backups will be deleted like regular backups (once fully implemented):

```bash
# Delete backups older than the specified backup
wal-g delete before snapshot_000000010000000000000004

# Retain only the last 5 backups
wal-g delete retain 5

# Delete a specific backup
wal-g delete target snapshot_000000010000000000000004
```

If `WALG_SNAPSHOT_DELETE_COMMAND` is configured, WAL-G will automatically execute it to clean up the snapshot.

**WAL Protection**: When deleting backups, WAL-G automatically protects the WAL files needed by any remaining snapshot backups. This means:
- You can safely use `delete retain N` without worrying about breaking snapshot backups
- WAL files from the snapshot's start LSN to finish LSN are preserved
- Only when the snapshot backup itself is deleted will its WAL files become eligible for deletion
- This protection applies to **all** snapshot backups, not just permanent ones

## Recovery

### Preparing Snapshot for Recovery with `snapshot-fetch`

After restoring the snapshot data, you **must** run `snapshot-fetch` to create the `backup_label` file that PostgreSQL needs for recovery.

#### Simple Usage (Manual Recovery Configuration)

```bash
# 1. Restore snapshot data to target directory
# (Example with AWS EBS - restore snapshot to volume and mount it)

# 2. Place backup_label and tablespace_map files
wal-g snapshot-fetch snapshot_000000010000000000000004 /var/lib/postgresql/data

# 3. Manually configure recovery (PostgreSQL 12+)
touch /var/lib/postgresql/data/recovery.signal
echo "restore_command = 'wal-g wal-fetch %f %p'" >> /var/lib/postgresql/data/postgresql.auto.conf

# 4. Start PostgreSQL
pg_ctl start -D /var/lib/postgresql/data
```

#### Advanced Usage (Automatic Recovery Configuration)

```bash
# 1. Restore snapshot data to target directory

# 2. Prepare the backup for recovery with automatic configuration
wal-g snapshot-fetch snapshot_000000010000000000000004 /var/lib/postgresql/data \
  --setup-recovery \
  --restore-command "wal-g wal-fetch %f %p"

# 3. Start PostgreSQL
pg_ctl start -D /var/lib/postgresql/data
```

**What `snapshot-fetch` does:**
- Fetches snapshot backup metadata from storage
- Writes the exact `backup_label` content that PostgreSQL provided during `pg_stop_backup()`
  - **Important**: The files are NOT reconstructed - we use the exact content from PostgreSQL to ensure compatibility across all versions
- Writes `tablespace_map` if tablespaces were used (also exact content from PostgreSQL)
- With `--setup-recovery` flag: Automatically configures recovery settings (creates `recovery.signal` or `recovery.conf` depending on PostgreSQL version)

**Command Options:**
- `--setup-recovery`: Configure recovery settings automatically (creates `recovery.signal` and updates `postgresql.auto.conf`)
- `--restore-command <cmd>`: Custom restore_command (default: `'wal-g wal-fetch %f %p'`) - requires `--setup-recovery`
- `--recovery-target <time>`: Point-in-time recovery target - requires `--setup-recovery`

### Full Recovery

**Step by step:**

1. **Restore the snapshot** (using your cloud provider's tools)
2. **Prepare for recovery** with `snapshot-fetch`
3. **Start PostgreSQL**

**Example with AWS EBS:**
```bash
# 1. Create volume from snapshot
VOLUME_ID=$(aws ec2 create-volume \
  --snapshot-id snap-xxx \
  --availability-zone us-east-1a \
  --query 'VolumeId' --output text)

# Attach and mount volume
aws ec2 attach-volume --volume-id $VOLUME_ID --instance-id i-xxx --device /dev/xvdf
mount /dev/xvdf /var/lib/postgresql/data

# 2. Prepare backup with snapshot-fetch
wal-g snapshot-fetch base_000000010000000000000004 /var/lib/postgresql/data \
  --setup-recovery

# 3. Start PostgreSQL
systemctl start postgresql
```

**Manual recovery configuration (without `--setup-recovery`):**
```bash
# Just create backup_label
wal-g snapshot-fetch base_000000010000000000000004 /var/lib/postgresql/data

# Then manually configure recovery:
# For PostgreSQL 12+:
touch /var/lib/postgresql/data/recovery.signal
echo "restore_command = 'wal-g wal-fetch %f %p'" >> /var/lib/postgresql/data/postgresql.conf

# For PostgreSQL 11 and earlier:
cat > /var/lib/postgresql/data/recovery.conf << EOF
restore_command = 'wal-g wal-fetch %f %p'
EOF

# Start PostgreSQL
pg_ctl start
```

### Point-in-Time Recovery (PITR)

To recover to a specific point in time, use `snapshot-fetch` with the `--recovery-target` option:

```bash
# 1. Restore snapshot (must be from before the target time)
# ...mount snapshot data to /var/lib/postgresql/data

# 2. Prepare with PITR target
wal-g snapshot-fetch base_000000010000000000000004 /var/lib/postgresql/data \
  --setup-recovery \
  --recovery-target '2025-11-01 10:30:00'

# 3. Start PostgreSQL
pg_ctl start -D /var/lib/postgresql/data
```

**Manual PITR configuration:**
```bash
# Create backup_label
wal-g snapshot-fetch base_000000010000000000000004 /var/lib/postgresql/data

# Configure PITR manually:
# For PostgreSQL 12+:
touch /var/lib/postgresql/data/recovery.signal
cat >> /var/lib/postgresql/data/postgresql.conf << EOF
restore_command = 'wal-g wal-fetch %f %p'
recovery_target_time = '2025-11-01 10:30:00'
recovery_target_action = 'promote'
EOF

# For PostgreSQL 11 and earlier:
cat > /var/lib/postgresql/data/recovery.conf << EOF
restore_command = 'wal-g wal-fetch %f %p'
recovery_target_time = '2025-11-01 10:30:00'
EOF

# Start PostgreSQL
pg_ctl start
```

**Alternative PITR targets:**
```bash
# Recover to specific LSN
--recovery-target '0/3000000'

# Recover to specific transaction ID
--recovery-target 'xid 12345'

# Recover to named restore point
--recovery-target 'my_restore_point'
```

## Best Practices

### 1. **WAL Archiving is Essential**

Snapshot backups **require** continuous WAL archiving for:
- Point-in-time recovery
- Recovery to any point after the snapshot
- Consistency verification

Configure WAL archiving:
```conf
# postgresql.conf
archive_mode = on
archive_command = 'wal-g wal-push %p'
```

**Important**: WAL files required by snapshot backups are **automatically protected** from deletion. When you run `wal-g delete` commands, WAL-G will:
- Identify all snapshot backups
- Calculate which WAL segments are needed (from start LSN to finish LSN)
- Preserve those WAL segments even if the retention policy would normally delete them
- Only delete the snapshot backup metadata when explicitly requested

This ensures snapshot backups remain recoverable even after aggressive retention policies.

### 2. **Test Your Snapshot Command**

Before using in production, verify your snapshot command works:

```bash
# Set the environment variables manually
export WALG_SNAPSHOT_NAME="test-$(date +%Y%m%d%H%M%S)"
export WALG_PG_DATA="/var/lib/postgresql/data"
export WALG_SNAPSHOT_START_LSN="0/3000000"

# Run your snapshot command
eval "$WALG_SNAPSHOT_COMMAND"
```

### 3. **Tag Your Snapshots**

Always tag snapshots with:
- Backup name
- Start LSN
- Application/database name
- Any other relevant metadata

This helps with:
- Identifying snapshots
- Automated cleanup
- Cost tracking

### 4. **Monitor Snapshot Costs**

Cloud provider snapshots incur storage costs. Monitor:
- Number of snapshots
- Snapshot age
- Storage usage
- Data change rate (affects incremental snapshot costs)

### 5. **Retention Policies**

Configure appropriate retention:

```bash
# Keep last 7 days
wal-g delete retain 7

# Keep specific backups permanent
wal-g snapshot-push /var/lib/postgresql/data --permanent
```

### 6. **Verify Recovery Procedures**

Regularly test recovery:
1. Create a test snapshot backup
2. Restore to a separate system
3. Verify data integrity
4. Practice PITR scenarios

## Technical Implementation Details

### Architecture Overview

Snapshot backups in WAL-G follow a **metadata-only** approach:

**During `snapshot-push`:**
1. WAL-G calls `pg_start_backup()` to put PostgreSQL in backup mode
2. Executes your custom `WALG_SNAPSHOT_COMMAND` with environment variables:
   - `WALG_SNAPSHOT_NAME`: Unique backup identifier
   - `WALG_PG_DATA`: PostgreSQL data directory path
   - `WALG_SNAPSHOT_START_LSN`: LSN when backup started
   - `WALG_SNAPSHOT_START_WAL_FILE`: Corresponding WAL file name
3. Calls `pg_stop_backup()` to complete the backup
4. Stores backup metadata in WAL-G storage (sentinel file)

**What's stored in WAL-G:**
- Backup metadata (LSNs, timestamps, PostgreSQL version)
- **Exact `backup_label` content** from `pg_stop_backup()` (NOT reconstructed)
- **Exact `tablespace_map` content** from `pg_stop_backup()` (if tablespaces exist)
- User-provided metadata

**What's NOT stored in WAL-G:**
- Database files (they remain in the snapshot)
- No file-level metadata tracking
- No compressed data uploads

### Critical: backup_label Handling

**Important Design Decision**: WAL-G stores the **exact** content that PostgreSQL returns from `pg_stop_backup()` for `backup_label` and `tablespace_map` files.

**Why this matters:**
- PostgreSQL's file format can vary between versions
- Reconstructing these files could lead to incompatibility
- Any future PostgreSQL format changes are automatically handled
- Guaranteed compatibility: PostgreSQL generated it, PostgreSQL can read it

**During `snapshot-fetch`:**
```go
// Uses stored content directly - NEVER reconstructs
err = os.WriteFile(backupLabelPath, []byte(*sentinel.BackupLabel), 0600)
```

This ensures compatibility with **all PostgreSQL versions** (9.x through 16+) and any future releases.

### Automatic WAL Protection

**Critical Safety Feature**: WAL segments required by snapshot backups are automatically protected from deletion.

**How it works:**
1. During any `delete` operation, WAL-G scans for snapshot backups
2. For each snapshot backup, calculates required WAL range (start LSN → finish LSN)
3. Marks those WAL segments as "permanent" (protected)
4. Applies to **all snapshot backups**, not just those marked permanent

**Behavior with retention policies:**
```bash
# These commands automatically preserve WAL files for snapshot backups:
wal-g delete retain 7      # Keeps WAL for remaining snapshots
wal-g delete before X      # Keeps WAL for snapshots after cutoff
wal-g delete target X      # Only deletes WAL when snapshot is deleted
```

**Why it's critical:**
- Snapshot backups don't store data files - they **require** WAL files for recovery
- Without WAL protection, snapshots could become unrecoverable
- Automatic protection means no manual configuration needed

**Implementation detail:**
```go
// In GetPermanentBackupsAndWals()
isSnapshotBackup := IsSnapshotBackup(backupName, sentinel)
if meta.IsPermanent || isSnapshotBackup {
    // Protect WAL segments from deletion
}
```

### Snapshot Identification

WAL-G identifies snapshot backups in the sentinel by:
- `FilesMetadataDisabled: true` - No file tracking
- `CompressedSize: 0` and `UncompressedSize: 0` - No data uploaded
- `BackupLabel != nil` - Contains stored backup_label content

### The snapshot-fetch Command

The `snapshot-fetch` command bridges the gap between external snapshot restoration and PostgreSQL recovery:

**Purpose**: After restoring snapshot data externally, this command prepares the data directory for PostgreSQL to start recovery.

**What it does:**
1. Fetches backup metadata from WAL-G storage
2. Writes the exact `backup_label` file (from stored content)
3. Writes `tablespace_map` if tablespaces were used
4. Optionally configures recovery settings

**Recovery configuration modes:**

*PostgreSQL 12+:*
- Creates `recovery.signal` file
- Appends to `postgresql.auto.conf`:
  ```
  restore_command = 'wal-g wal-fetch %f %p'
  recovery_target_time = '...'  # if PITR
  ```

*PostgreSQL 11 and earlier:*
- Creates `recovery.conf`:
  ```
  restore_command = 'wal-g wal-fetch %f %p'
  recovery_target_time = '...'  # if PITR
  ```

**Version detection**: Automatically reads `PG_VERSION` file to configure recovery correctly.

### Command Execution Security

**Snapshot commands execute in shell context:**
- Uses `/bin/sh -c` for flexibility
- Environment variables passed to command
- Command output captured and logged
- Exit codes checked for errors

**Security considerations:**
- Secure credentials using cloud provider tools (IAM roles, managed identities)
- Validate environment variables in your scripts
- Test commands thoroughly before production use
- Use least-privilege permissions

### Error Handling

**Snapshot command failures:**
- If snapshot command fails, `pg_stop_backup()` is still called
- Ensures PostgreSQL doesn't remain in backup mode
- Error details logged for debugging

**Delete command failures:**
- Logged as warnings but don't block deletion
- Allows WAL-G metadata cleanup even if external snapshot cleanup fails
- Manual cleanup may be required

**Recovery failures:**
- `snapshot-fetch` validates backup is actually a snapshot
- Checks metadata exists and is accessible
- Provides clear error messages for missing files

### Performance Characteristics

**Backup speed:**
- `pg_start_backup()` / `pg_stop_backup()`: Milliseconds
- Snapshot command: Varies (typically seconds for cloud snapshots)
- Metadata upload: Milliseconds (very small JSON file)
- **Total**: Usually < 30 seconds regardless of database size

**Recovery speed:**
- Snapshot restoration: Depends on cloud provider/storage system
- `snapshot-fetch`: Milliseconds (just writes small files)
- PostgreSQL recovery: Depends on WAL volume to replay

**Storage efficiency:**
- WAL-G storage: Only metadata (~1-10 KB per backup)
- Snapshot storage: Full database size (managed externally)
- WAL archive: Shared with regular backups

### Integration with WAL-G Features

**Compatible with:**
- ✅ Multiple storage backends (S3, Azure, GCP, etc.)
- ✅ Encryption (for WAL files)
- ✅ Compression (for WAL files)
- ✅ `backup-list` command (snapshots appear in list)
- ✅ Retention policies (`delete` commands)
- ✅ Permanent backups flag
- ✅ User metadata attachment

**Not applicable to snapshots:**
- ❌ Delta/incremental backups (snapshots are always full)
- ❌ File-level deduplication
- ❌ Parallel upload (no data upload)
- ❌ Compression levels (no data to compress)

### File Structure in Storage

```
basebackups_005/
├── base_000000010000000000000003/
│   └── backup_sentinel.json          # Metadata with backup_label content
├── base_000000010000000000000004/
│   └── backup_sentinel.json
└── wal_005/
    ├── 000000010000000000000003.lz4   # WAL files (protected)
    ├── 000000010000000000000004.lz4
    └── ...
```

**Note**: No tar files for snapshot backups - only sentinel metadata.

## Limitations

1. **Standby Servers**: Snapshot backups cannot be created on standby servers (they must be in recovery mode check)
2. **Cross-Region**: Snapshot restore is typically limited to the same region/availability zone
3. **Tablespaces**: All tablespaces must be on the same volume/filesystem being snapshotted
4. **Consistency**: The snapshot command must be fast enough to avoid long backup windows

## Troubleshooting

### "Snapshot command is not configured"

**Problem**: `WALG_SNAPSHOT_COMMAND` is not set.

**Solution**: Set the environment variable:
```bash
export WALG_SNAPSHOT_COMMAND='your-snapshot-command'
```

### "Cannot perform snapshot backup on a standby server"

**Problem**: Trying to create a snapshot on a standby PostgreSQL server.

**Solution**: Snapshot backups must be created on the primary server. Use regular backups for standbys or create snapshots at the storage level independently.

### "pg_start_backup() failed"

**Problem**: PostgreSQL rejected the backup start request.

**Possible Causes:**
- Insufficient permissions
- Another backup in progress
- Database in recovery mode

**Solution**: Check PostgreSQL logs and ensure the database is in a normal operating state.

### Snapshot command fails

**Problem**: The snapshot command exits with an error.

**Troubleshooting Steps:**
1. Test the command manually
2. Check cloud provider permissions
3. Verify volume/disk IDs
4. Check for rate limits or quota issues
5. Review command output in WAL-G logs

## Examples

### Complete AWS EBS Snapshot Workflow

```bash
# Configuration
export WALG_STORAGE_PREFIX="s3://my-walg-bucket"
export WALG_SNAPSHOT_COMMAND='aws ec2 create-snapshot \
  --volume-id vol-abc123 \
  --description "WAL-G Snapshot: $WALG_SNAPSHOT_NAME" \
  --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=$WALG_SNAPSHOT_NAME}]"'
export WALG_SNAPSHOT_DELETE_COMMAND='SNAPSHOT_ID=$(aws ec2 describe-snapshots \
  --filters "Name=tag:Name,Values=$WALG_SNAPSHOT_NAME" \
  --query "Snapshots[0].SnapshotId" --output text) && \
  [ "$SNAPSHOT_ID" != "None" ] && aws ec2 delete-snapshot --snapshot-id $SNAPSHOT_ID'

# Create backup
wal-g snapshot-push /var/lib/postgresql/14/main

# List backups
wal-g backup-list

# Delete old backups (will clean up snapshots automatically)
wal-g delete retain 7
```

### Azure Managed Disk Snapshot

```bash
export WALG_STORAGE_PREFIX="azure://mycontainer"
export AZURE_STORAGE_ACCOUNT="myaccount"
export AZURE_STORAGE_ACCESS_KEY="mykey"

export WALG_SNAPSHOT_COMMAND='az snapshot create \
  --resource-group myRG \
  --name walg-$WALG_SNAPSHOT_NAME \
  --source myDisk \
  --tags backup=$WALG_SNAPSHOT_NAME lsn=$WALG_SNAPSHOT_START_LSN'

export WALG_SNAPSHOT_DELETE_COMMAND='az snapshot delete \
  --resource-group myRG \
  --name walg-$WALG_SNAPSHOT_NAME'

wal-g snapshot-push /var/lib/postgresql/data
```

## See Also

- [PostgreSQL Documentation](PostgreSQL.md)
- [Storage Configuration](STORAGES.md)
- [WAL Archiving Best Practices](PostgreSQL.md#wal-archiving)

