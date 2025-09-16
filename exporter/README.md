# WAL-G Prometheus Exporter

A Prometheus exporter for WAL-G backup and WAL metrics for PostgreSQL databases.

## Features

- **Backup completion timestamps**: Track completion timestamps of backup-push operations (full and delta backups)
- **WAL completion timestamps**: Monitor completion timestamps of wal-push operations
- **LSN delta lag**: Calculate LSN lag in bytes between current and archived WAL
- **PITR window**: Monitor point-in-time recovery window size
- **Error monitoring**: Track WAL-G operation errors
- **WAL integrity**: Monitor WAL segment integrity status per timeline

## Metrics

The exporter provides the following metrics:

### Backup Metrics
- `walg_backup_timestamp{backup_name, backup_type, wal_file, start_lsn, finish_lsn, permanent}` - Unix timestamp of backup **completion** (when backup finished successfully)
- `walg_backup_count{backup_type}` - Number of successful backups (full/delta)

### WAL Metrics
- `walg_wal_timestamp{timeline}` - Unix timestamp of WAL segment **completion** (when wal-push finished successfully)
- `walg_lsn_lag_bytes{timeline}` - LSN delta lag in bytes
- `walg_wal_integrity_status{timeline}` - WAL integrity status (1 = OK, 0 = ERROR)

### PITR Metrics
- `walg_pitr_window_seconds` - Point-in-time recovery window size in seconds

### Error Metrics
- `walg_errors_total{operation,error_type}` - Total number of WAL-G errors

### Exporter Metrics
- `walg_scrape_duration_seconds` - Duration of the last scrape
- `walg_scrape_errors_total` - Total number of scrape errors

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
- `--log.level` - Log level (default: `info`)

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
(time() - walg_backup_timestamp) / 3600

# Show only full backups completion time
(time() - walg_backup_timestamp{backup_type="full"}) / 3600
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

### Backup Timeline
```promql
# Show backup completion timestamps as time series
walg_backup_timestamp * 1000  # Convert to milliseconds for Grafana
```

## Timestamp Semantics

**Important**: All timestamp metrics represent **completion times**, not start times.

### Backup Timestamps
- `walg_backup_timestamp` = Time when the backup operation **finished successfully**
- This corresponds to when WAL-G writes the `_backup_stop_sentinel.json` file
- Failed or interrupted backups do not generate timestamps
- Represents the moment when the backup became available for recovery

### WAL Timestamps  
- `walg_wal_timestamp` = Time when the WAL segment **finished uploading**
- This is when the WAL segment became available in storage
- Represents the completion of the wal-push operation

### Why Completion Times?
- **Recovery Point Objective (RPO)**: You care when data was successfully backed up
- **Alerting**: Know how long since you had a complete, usable backup
- **PITR calculations**: Recovery depends on when backups/WAL completed, not when they started

## Development

### Running Tests

```bash
go test -v ./...
```

### Running Benchmarks

```bash
go test -bench=. -benchmem ./...
```

### Mock Testing

The exporter includes comprehensive tests with mock WAL-G commands:

```bash
# Create a mock wal-g script
cat > mock-wal-g << 'EOF'
#!/bin/bash
case "$1" in
  "backup-list")
    echo '[{"backup_name":"test","time":"2024-01-01T12:00:00Z","is_full":true}]'
    ;;
  "wal-show")
    echo '{"integrity":{"status":"OK","details":[{"timeline_id":1,"status":"FOUND"}]}}'
    ;;
esac
EOF
chmod +x mock-wal-g

# Test with mock
./walg-exporter --walg.path=./mock-wal-g
```

## Architecture

The exporter consists of several components:

- **main.go**: HTTP server and command-line interface
- **exporter.go**: Core Prometheus collector implementation
- **wal_lag.go**: LSN parsing and WAL lag calculation
- **pitr.go**: PITR window calculation logic

### LSN Parsing

The exporter includes a full LSN parser that handles PostgreSQL LSN format:

```go
lsn, err := ParseLSN("0/1A2B3C4D")
fmt.Println(lsn.String()) // "0/1A2B3C4D"
fmt.Println(lsn.Bytes())  // 439041101
```

### Lag Calculation

WAL and LSN lag calculations:

```go
// Time-based lag
walLag := calculateWalLag(lastWalTime)

// LSN-based lag in bytes
lsnLag := calculateLSNLag(currentLSN, lastArchivedLSN)
```

## Troubleshooting

### Common Issues

1. **WAL-G command not found**
   - Ensure WAL-G is in PATH or specify with `--walg.path`
   - Check that WAL-G is executable

2. **Permission denied**
   - Ensure the exporter has permission to execute WAL-G
   - Check WAL-G configuration file permissions

3. **No metrics**
   - Verify WAL-G commands work manually
   - Check exporter logs for errors
   - Ensure WAL-G is properly configured

### Debug Mode

Enable debug logging:

```bash
./walg-exporter --log.level=debug
```

### Health Check

The exporter provides a health endpoint:

```bash
curl http://localhost:9351/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the same license as WAL-G.

## Support

For issues and questions:
- Check the [WAL-G documentation](https://github.com/wal-g/wal-g)
- File an issue in the WAL-G repository
- Join the WAL-G community discussions 