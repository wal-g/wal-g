# WAL-G Prometheus Exporter

A comprehensive Prometheus exporter for WAL-G backup and WAL monitoring for PostgreSQL databases.

## Features

- **Accurate backup type detection**: Uses WAL-G's actual naming conventions to distinguish full vs incremental backups
- **Separate start/finish timestamps**: Track both when backups started and when they completed
- **Base backup tracking**: For incremental backups, shows which full backup they're based on
- **WAL monitoring**: Track WAL segment current timeline and integrity per timeline
- **PITR window**: Calculate point-in-time recovery window size
- **Storage health**: Monitor storage connectivity and latency
- **Error tracking**: Comprehensive WAL-G operation error monitoring

## Metrics

The exporter provides the following metrics:

### Backup Metrics

- `walg_backup_info{backup_name, backup_type, wal_file, pg_version, start_lsn, finish_lsn, is_permanent, delta_origin}`: Information about stored backups. Value is always 1.
- `walg_backup_start_timestamp{backup_name}`: Unix timestamp when backup started.
- `walg_backup_finish_timestamp{backup_name}`: Unix timestamp when backup completed successfully.
- `walg_backup_compressed_size_bytes{backup_name}`: Compressed size of the backup in bytes.
- `walg_backup_uncompressed_size_bytes{backup_name}`: Uncompressed size of the backup in bytes.
- `walg_backups{backup_type}`: Number of successful backups (full/delta).

**Label Details:**

- `backup_name`: Full backup name (e.g., `base_000000010000000000000025` or `base_000000010000000500000007_D_000000010000000000000025`)
- `backup_type`: `full` or `delta` (determined by presence of `_D_` in backup name)
- `delta_origin`: For delta backups, shows the base backup name they're built on (empty for full backups)
  - Full Backup ← Delta 1 ← Delta 2 ← Delta 3 (chaining increments)
  - Full Backup (Base) ← Delta N

### WAL Metrics

- `walg_wal_verify_status{operation}`: WAL verify status (1 = OK, 0 = FAILURE, 2 = WARNING, -1 = UNKNOWN)
- `walg_wal_integrity_status{timeline_id, timeline_hex}`: WAL integrity status (1 = FOUND, 0 = MISSING)

### Storage Metrics

- `walg_storage_up`: Storage connectivity status (1 = up, 0 = down)
- `walg_storage_latency_seconds`: Storage operation latency

### PITR Metrics

- `walg_pitr_window_seconds` - Point-in-time recovery window size in seconds

### Error Metrics

- `walg_errors_total{operation,error_type}` - Total number of WAL-G errors

### Exporter Metrics

- `walg_backup_list_duration_seconds` - Time taken to execute 'backup-list' during the last collector run
- `walg_wal_verify_duration_seconds` - Time taken to execute 'wal-verify' during the last collector run
- `walg_scrape_errors_total` - Total number of scrape errors

## Backup Type Detection

The exporter correctly identifies backup types using WAL-G's actual naming conventions:

- **Full backups**: Names like `base_000000010000000000000025` (no `_D_` suffix)
- **Incremental/Delta backups**: Names like `base_000000010000000500000007_D_000000010000000000000025` (contains `_D_`)
  - The part after `_D_` indicates the base backup: `000000010000000000000025` → `base_000000010000000000000025`

## Installation

### From Source

```bash
cd cmd/pg/exporter/
go build -o walg-exporter .
```

## Usage

### Command Line Options

```bash
./walg-exporter [flags]
```

**Flags:**

- `-backup-list.scrape-interval` duration  
    Interval between backup-list scrapes. (default 1m0s)
- `-storage-check.scrape-interval` duration  
    Interval between storage scrapes. (default 30s)
- `-wal-verify.scrape-interval` duration  
    Interval between wal-verify scrapes. (default 5m0s)
- `-walg.config-path` string  
    Path to the wal-g config file.
- `-walg.path` string  
    Path to the wal-g binary. (default "wal-g")
- `-web.listen-address` string  
    Address to listen on for web interface and telemetry. (default ":9351")
- `-web.telemetry-path` string  
    Path under which to expose metrics. (default "/metrics")

### Example

```bash
# Basic usage
./walg-exporter

# Custom configuration
./walg-exporter \
  -web.listen-address=":8080" \
  -walg.path="/opt/bin/wal-g" \
  -backup-list.scrape-interval="10m" \
  -wal-verify.scrape-interval="3h" \
  -storage-check.scrape-interval="3m"
```

## Configuration

The exporter requires WAL-G to be properly configured and accessible. Ensure that:

1. WAL-G is installed and in PATH (or specify with `--walg.path`)
2. WAL-G configuration is properly set up (environment variables, config file)
3. The exporter has access to execute WAL-G commands

### WAL-G Commands Used

The exporter executes the following WAL-G commands:

- `wal-g backup-list --detail --json`: Lists currently available backups in storage.
- `wal-g wal-verify integrity timeline --json`: Checks to ensure that WAL segment storage is healthy.
- `wal-g st check read`: Check storage connectivity.

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
(time() - (walg_backup_finish_timestamp * on(backup_name) group_left(backup_type) walg_backup_info{backup_type="full"})) / 3600

# Show incremental backups and their base backups
walg_backup_info{backup_type="delta"} * on(delta_origin) group_left(backup_name) walg_backup_info{backup_type="full"}
```

### Backup Duration

```promql
# How long backups took to complete
walg_backup_finish_timestamp - walg_backup_start_timestamp
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
walg_storage_up

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
./walg-exporter -walg.path=./mock-wal-g -backup-list.scrape-interval=10s
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

### Sample Backup Metrics

```text
# HELP walg_backup_compressed_size_bytes Compressed size of the backup in bytes.
# TYPE walg_backup_compressed_size_bytes gauge
walg_backup_compressed_size_bytes{backup_name="base_000000430000030600000060"} 8.5850239e+08
walg_backup_compressed_size_bytes{backup_name="base_000000430000035900000062"} 9.02940802e+08
walg_backup_compressed_size_bytes{backup_name="base_0000004300000371000000AB"} 9.17213565e+08
walg_backup_compressed_size_bytes{backup_name="base_000000430000037B00000083"} 9.23384637e+08
walg_backup_compressed_size_bytes{backup_name="base_000000430000038500000003"} 9.2791466e+08
walg_backup_compressed_size_bytes{backup_name="base_0000004300000388000000AC_D_000000430000038500000003"} 3.86034756e+08
walg_backup_compressed_size_bytes{backup_name="base_0000004300000388000000BB_D_0000004300000388000000AC"} 2.09052691e+08
walg_backup_compressed_size_bytes{backup_name="base_0000004300000388000000BD"} 9.32195603e+08
walg_backup_compressed_size_bytes{backup_name="base_0000004300000388000000BF_D_0000004300000388000000BD"} 711409
walg_backup_compressed_size_bytes{backup_name="base_0000004300000388000000C1_D_0000004300000388000000BF"} 907716
walg_backup_compressed_size_bytes{backup_name="base_0000004400000388000000C7"} 9.31763965e+08
# HELP walg_backup_finish_timestamp Finish time of the backup (Unix timestamp).
# TYPE walg_backup_finish_timestamp gauge
walg_backup_finish_timestamp{backup_name="base_000000430000030600000060"} 1.766196018e+09
walg_backup_finish_timestamp{backup_name="base_000000430000035900000062"} 1.766714419e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000371000000AB"} 1.766887218e+09
walg_backup_finish_timestamp{backup_name="base_000000430000037B00000083"} 1.766973617e+09
walg_backup_finish_timestamp{backup_name="base_000000430000038500000003"} 1.767060021e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000388000000AC_D_000000430000038500000003"} 1.76708922e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000388000000BB_D_0000004300000388000000AC"} 1.767089411e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000388000000BD"} 1.767089442e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000388000000BF_D_0000004300000388000000BD"} 1.767089525e+09
walg_backup_finish_timestamp{backup_name="base_0000004300000388000000C1_D_0000004300000388000000BF"} 1.767089602e+09
walg_backup_finish_timestamp{backup_name="base_0000004400000388000000C7"} 1.767089767e+09
# HELP walg_backup_info Information about stored backups. Value is always 1.
# TYPE walg_backup_info gauge
walg_backup_info{backup_name="base_000000430000030600000060",backup_type="full",delta_origin="",finish_lsn="306/600BEAC0",is_permanent="false",pg_version="150015",start_lsn="306/60000028",wal_file="000000430000030600000060"} 1
walg_backup_info{backup_name="base_000000430000035900000062",backup_type="full",delta_origin="",finish_lsn="359/620DB728",is_permanent="false",pg_version="150015",start_lsn="359/62000028",wal_file="000000430000035900000062"} 1
walg_backup_info{backup_name="base_0000004300000371000000AB",backup_type="full",delta_origin="",finish_lsn="371/AB0C7F40",is_permanent="false",pg_version="150015",start_lsn="371/AB000028",wal_file="0000004300000371000000AB"} 1
walg_backup_info{backup_name="base_000000430000037B00000083",backup_type="full",delta_origin="",finish_lsn="37B/830D5690",is_permanent="false",pg_version="150015",start_lsn="37B/83000028",wal_file="000000430000037B00000083"} 1
walg_backup_info{backup_name="base_000000430000038500000003",backup_type="full",delta_origin="",finish_lsn="385/30C4AA0",is_permanent="false",pg_version="150015",start_lsn="385/30025B8",wal_file="000000430000038500000003"} 1
walg_backup_info{backup_name="base_0000004300000388000000AC_D_000000430000038500000003",backup_type="delta",delta_origin="base_000000430000038500000003",finish_lsn="388/AC06D190",is_permanent="false",pg_version="150015",start_lsn="388/AC0000D8",wal_file="0000004300000388000000AC"} 1
walg_backup_info{backup_name="base_0000004300000388000000BB_D_0000004300000388000000AC",backup_type="delta",delta_origin="base_0000004300000388000000AC_D_000000430000038500000003",finish_lsn="388/BB02B7D0",is_permanent="false",pg_version="150015",start_lsn="388/BB000028",wal_file="0000004300000388000000BB"} 1
walg_backup_info{backup_name="base_0000004300000388000000BD",backup_type="full",delta_origin="",finish_lsn="388/BD025830",is_permanent="false",pg_version="150015",start_lsn="388/BD000028",wal_file="0000004300000388000000BD"} 1
walg_backup_info{backup_name="base_0000004300000388000000BF_D_0000004300000388000000BD",backup_type="delta",delta_origin="base_0000004300000388000000BD",finish_lsn="388/BF001F18",is_permanent="false",pg_version="150015",start_lsn="388/BF000028",wal_file="0000004300000388000000BF"} 1
walg_backup_info{backup_name="base_0000004300000388000000C1_D_0000004300000388000000BF",backup_type="delta",delta_origin="base_0000004300000388000000BF_D_0000004300000388000000BD",finish_lsn="388/C100A418",is_permanent="false",pg_version="150015",start_lsn="388/C1000028",wal_file="0000004300000388000000C1"} 1
walg_backup_info{backup_name="base_0000004400000388000000C7",backup_type="full",delta_origin="",finish_lsn="388/C70DD4F0",is_permanent="false",pg_version="150015",start_lsn="388/C7000028",wal_file="0000004400000388000000C7"} 1
# HELP walg_backup_list_duration_seconds Time taken to execute 'backup-list' during the last collector run.
# TYPE walg_backup_list_duration_seconds gauge
walg_backup_list_duration_seconds 2.007721887
# HELP walg_backup_start_timestamp Start time of the backup (Unix timestamp).
# TYPE walg_backup_start_timestamp gauge
walg_backup_start_timestamp{backup_name="base_000000430000030600000060"} 1.766196001e+09
walg_backup_start_timestamp{backup_name="base_000000430000035900000062"} 1.766714401e+09
walg_backup_start_timestamp{backup_name="base_0000004300000371000000AB"} 1.766887202e+09
walg_backup_start_timestamp{backup_name="base_000000430000037B00000083"} 1.766973601e+09
walg_backup_start_timestamp{backup_name="base_000000430000038500000003"} 1.767060002e+09
walg_backup_start_timestamp{backup_name="base_0000004300000388000000AC_D_000000430000038500000003"} 1.767089211e+09
walg_backup_start_timestamp{backup_name="base_0000004300000388000000BB_D_0000004300000388000000AC"} 1.767089402e+09
walg_backup_start_timestamp{backup_name="base_0000004300000388000000BD"} 1.767089429e+09
walg_backup_start_timestamp{backup_name="base_0000004300000388000000BF_D_0000004300000388000000BD"} 1.767089523e+09
walg_backup_start_timestamp{backup_name="base_0000004300000388000000C1_D_0000004300000388000000BF"} 1.767089601e+09
walg_backup_start_timestamp{backup_name="base_0000004400000388000000C7"} 1.767089721e+09
# HELP walg_backup_uncompressed_size_bytes Uncompressed size of the backup in bytes.
# TYPE walg_backup_uncompressed_size_bytes gauge
walg_backup_uncompressed_size_bytes{backup_name="base_000000430000030600000060"} 2.604967632e+09
walg_backup_uncompressed_size_bytes{backup_name="base_000000430000035900000062"} 2.716157648e+09
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000371000000AB"} 2.753382094e+09
walg_backup_uncompressed_size_bytes{backup_name="base_000000430000037B00000083"} 2.76925e+09
walg_backup_uncompressed_size_bytes{backup_name="base_000000430000038500000003"} 2.782971598e+09
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000388000000AC_D_000000430000038500000003"} 9.2877212e+08
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000388000000BB_D_0000004300000388000000AC"} 3.99392194e+08
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000388000000BD"} 2.792572624e+09
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000388000000BF_D_0000004300000388000000BD"} 2.033506e+06
walg_backup_uncompressed_size_bytes{backup_name="base_0000004300000388000000C1_D_0000004300000388000000BF"} 2.238281e+06
walg_backup_uncompressed_size_bytes{backup_name="base_0000004400000388000000C7"} 2.79139381e+09
# HELP walg_backups Number of backups by type
# TYPE walg_backups gauge
walg_backups{backup_type="delta"} 4
walg_backups{backup_type="full"} 7
# HELP walg_pitr_window_seconds Point-in-time recovery window size in seconds
# TYPE walg_pitr_window_seconds gauge
walg_pitr_window_seconds 1.158221840144585e+06
# HELP walg_scrape_errors_total Total number of scrape errors
# TYPE walg_scrape_errors_total counter
walg_scrape_errors_total 0
# HELP walg_storage_latency_seconds Storage operation latency in seconds
# TYPE walg_storage_latency_seconds gauge
walg_storage_latency_seconds 0.619920539
# HELP walg_storage_up Storage connectivity status (1 = up, 0 = down)
# TYPE walg_storage_up gauge
walg_storage_up 1
# HELP walg_wal_integrity_status WAL integrity status (1 = FOUND, 0 = MISSING)
# TYPE walg_wal_integrity_status gauge
walg_wal_integrity_status{timeline_hex="00000043",timeline_id="67"} 0
walg_wal_integrity_status{timeline_hex="00000044",timeline_id="68"} 1
# HELP walg_wal_verify_duration_seconds Time taken to execute 'wal-verify' during the last collector run.
# TYPE walg_wal_verify_duration_seconds gauge
walg_wal_verify_duration_seconds 2.007257456
# HELP walg_wal_verify_status WAL verify status (1 = OK, 0 = FAILURE, 2 = WARNING, -1 = UNKNOWN)
# TYPE walg_wal_verify_status gauge
walg_wal_verify_status{operation="integrity"} 0
walg_wal_verify_status{operation="timeline"} 1
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the same license as WAL-G.
