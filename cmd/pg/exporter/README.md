# WAL-G Prometheus Exporter

A comprehensive Prometheus exporter for WAL-G backup and WAL monitoring for PostgreSQL databases.

## Features

- **Accurate backup type detection**: Uses WAL-G's actual naming conventions to distinguish full vs incremental backups
- **Separate start/finish timestamps**: Track both when backups started and when they completed
- **Base backup tracking**: For incremental backups, shows which full backup they're based on
- **WAL monitoring**: Track WAL segment timestamps and integrity per timeline
- **LSN lag calculation**: Monitor LSN lag in bytes between current and archived WAL
- **PITR window**: Calculate point-in-time recovery window size
- **Storage health**: Monitor storage connectivity and latency
- **Error tracking**: Comprehensive WAL-G operation error monitoring

## Metrics

The exporter provides the following metrics:

### Backup Metrics
- `walg_backup_start_timestamp{backup_name, backup_type, wal_file, start_lsn, finish_lsn, permanent, base_backup}` - Unix timestamp when backup started
- `walg_backup_finish_timestamp{backup_name, backup_type, wal_file, start_lsn, finish_lsn, permanent, base_backup}` - Unix timestamp when backup completed successfully
- `walg_backup_count{backup_type}` - Number of successful backups (full/delta)

**Label Details:**
- `backup_name`: Full backup name (e.g., `base_000000010000000000000025` or `base_000000010000000500000007_D_000000010000000000000025`)
- `backup_type`: `full` or `delta` (determined by presence of `_D_` in backup name)
- `base_backup`: For delta backups, shows the base backup name they're built on (empty for full backups)

### WAL Metrics
- `walg_wal_timestamp{timeline}` - Unix timestamp of WAL segment completion
- `walg_lsn_lag_bytes{timeline}` - LSN delta lag in bytes
- `walg_wal_integrity_status{timeline}` - WAL integrity status (1 = OK, 0 = ERROR)

### Storage Metrics
- `walg_storage_alive` - Storage connectivity status (1 = alive, 0 = dead)
- `walg_storage_latency_seconds` - Storage operation latency

### PITR Metrics
- `walg_pitr_window_seconds` - Point-in-time recovery window size in seconds

### Error Metrics
- `walg_errors_total{operation,error_type}` - Total number of WAL-G errors

### Exporter Metrics
- `walg_scrape_duration_seconds` - Duration of the last scrape
- `walg_scrape_errors_total` - Total number of scrape errors

## Backup Type Detection

The exporter correctly identifies backup types using WAL-G's actual naming conventions:

- **Full backups**: Names like `base_000000010000000000000025` (no `_D_` suffix)
- **Incremental/Delta backups**: Names like `base_000000010000000500000007_D_000000010000000000000025` (contains `_D_`)
  - The part after `_D_` indicates the base backup: `000000010000000000000025` → `base_000000010000000000000025`

This fixes the common issue where all backups were incorrectly marked as "full" based on the `base_` prefix.

## Installation

### From Source

```bash
cd exporter
go build -o walg-exporter .
```

### Using Docker

```bash
docker build -t walg-exporter .
docker run -p 9351:9351 walg-exporter
```

## Usage

### Command Line Options

```bash
./walg-exporter [flags]
```

**Flags:**
- `--web.listen-address` - Address to listen on (default: `:9351`)
- `--web.telemetry-path` - Path for metrics endpoint (default: `/metrics`)
- `--walg.path` - Path to wal-g binary (default: `wal-g`)
- `--scrape.interval` - Scrape interval (default: `60s`)

### Example

```bash
# Basic usage
./walg-exporter

# Custom configuration
./walg-exporter \
  --web.listen-address=":8080" \
  --walg.path="/usr/local/bin/wal-g" \
  --scrape.interval="30s"
```

## Configuration

The exporter requires WAL-G to be properly configured and accessible. Ensure that:

1. WAL-G is installed and in PATH (or specify with `--walg.path`)
2. WAL-G configuration is properly set up (environment variables, config file)
3. The exporter has access to execute WAL-G commands

### WAL-G Commands Used

The exporter executes the following WAL-G commands:
- `wal-g backup-list --detail --json` - Get backup information
- `wal-g wal-show --detailed-json` - Get WAL segment information
- `wal-g st ls` - Check storage connectivity

## Prometheus Configuration

Add the following to your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: 'walg-exporter'
    static_configs:
      - targets: ['localhost:9351']
    scrape_interval: 60s
    metrics_path: /metrics
```

## Grafana Dashboard

Example Grafana queries:

### Backup Age (in hours)
```promql
# Time since last backup completed
(time() - walg_backup_finish_timestamp) / 3600

# Show only full backups completion time
(time() - walg_backup_finish_timestamp{backup_type="full"}) / 3600

# Show incremental backups and their base backups
walg_backup_finish_timestamp{backup_type="delta"} * on(base_backup) group_left walg_backup_finish_timestamp{backup_type="full"}
```

### Backup Duration
```promql
# How long backups took to complete
walg_backup_finish_timestamp - walg_backup_start_timestamp
```

### WAL Age (in minutes)
```promql
# Time since last WAL segment completed
(time() - walg_wal_timestamp) / 60
```

### PITR Window (in hours)
```promql
walg_pitr_window_seconds / 3600
```

### Error Rate
```promql
rate(walg_errors_total[5m])
```

### Storage Health
```promql
# Storage connectivity
walg_storage_alive

# Storage latency
walg_storage_latency_seconds
```

## Timestamp Semantics

The exporter provides both start and finish timestamps for comprehensive backup monitoring:

### Backup Timestamps
- `walg_backup_start_timestamp` = When the backup operation **started**
- `walg_backup_finish_timestamp` = When the backup operation **finished successfully**
  - This corresponds to when WAL-G writes the `_backup_stop_sentinel.json` file
  - Failed or interrupted backups do not generate finish timestamps
  - Represents the moment when the backup became available for recovery

### WAL Timestamps  
- `walg_wal_timestamp` = When the WAL segment **finished uploading**
  - This is when the WAL segment became available in storage
  - Represents the completion of the wal-push operation

### Why Both Start and Finish Times?
- **Backup Duration**: Calculate how long backups take with `finish - start`
- **Recovery Point Objective (RPO)**: Use finish time to know when data was successfully backed up
- **Alerting**: Know how long since you had a complete, usable backup
- **Performance Monitoring**: Track backup performance trends

## Development

### Running Tests

```bash
go test -v ./...
```

### Mock Testing

The exporter includes comprehensive tests with mock WAL-G commands:

```bash
# Test with included mock script
./walg-exporter --walg.path=./mock-wal-g --scrape.interval=10s
```

The mock script generates realistic test data including:
- Full backups: `base_TIMESTAMP`
- Incremental backups: `base_TIMESTAMP_D_BASEBACKUP`
- Proper start/finish timestamps
- Realistic LSN values

## Architecture

The exporter consists of several components:

- **main.go**: HTTP server and command-line interface
- **exporter.go**: Core Prometheus collector implementation with accurate backup type detection
- **mock-wal-g**: Mock WAL-G script for testing

### Backup Type Detection Logic

```go
// IsFullBackup checks if backup is full based on WAL-G naming convention
func (b *BackupInfo) IsFullBackup() bool {
    // Incremental/delta backups have "_D_" in their name
    return !strings.Contains(b.BackupName, "_D_")
}

// GetBaseBackupName extracts base backup for delta backups
func (b *BackupInfo) GetBaseBackupName() string {
    if b.IsFullBackup() {
        return "" // Full backups don't have a base backup
    }
    
    // Extract identifier after "_D_" and prepend "base_"
    deltaIndex := strings.Index(b.BackupName, "_D_")
    baseIdentifier := b.BackupName[deltaIndex+3:]
    return "base_" + baseIdentifier
}
```

## Troubleshooting

### Common Issues

1. **Incorrect backup types**
   - ✅ **Fixed**: The exporter now correctly identifies backup types using `_D_` suffix detection
   - Old issue: All backups were marked as "full" due to `base_` prefix

2. **WAL-G command not found**
   - Ensure WAL-G is in PATH or specify with `--walg.path`
   - Check that WAL-G is executable

3. **Permission denied**
   - Ensure the exporter has permission to execute WAL-G
   - Check WAL-G configuration file permissions

4. **No metrics**
   - Verify WAL-G commands work manually
   - Check exporter logs for errors
   - Ensure WAL-G is properly configured

### Health Check

The exporter provides a health endpoint:

```bash
curl http://localhost:9351/
```

## Sample Output

### Backup Metrics
```
walg_backup_start_timestamp{backup_name="base_000000010000000000000025",backup_type="full",base_backup="",wal_file="000000010000000000000025",...} 1758055800
walg_backup_finish_timestamp{backup_name="base_000000010000000000000025",backup_type="full",base_backup="",wal_file="000000010000000000000025",...} 1758055900

walg_backup_start_timestamp{backup_name="base_000000010000000500000007_D_000000010000000000000025",backup_type="delta",base_backup="base_000000010000000000000025",...} 1758059400
walg_backup_finish_timestamp{backup_name="base_000000010000000500000007_D_000000010000000000000025",backup_type="delta",base_backup="base_000000010000000000000025",...} 1758059450
```

This shows:
- A full backup that took 100 seconds (1758055900 - 1758055800)
- An incremental backup based on the full backup that took 50 seconds
- Clear relationship between incremental and its base backup

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the same license as WAL-G.