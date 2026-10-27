# WAL-G MySQL/MariaDB Prometheus exporter

A Prometheus exporter for monitoring WAL-G backups and binlogs of MySQL and MariaDB. It does not talk to the database:
it runs the `wal-g` binary (`backup-list --detail --json`, `binlog-list --json`, `st check read`) and exposes the
result as metrics.

## Build and run

The exporter is a separate Go module:

```bash
cd cmd/mysql/exporter
go build -o walg-mysql-exporter .

./walg-mysql-exporter --walg.path=/usr/local/bin/wal-g --walg.config-path=/etc/wal-g/config.json
```

Metrics are served on `http://<listen-address>/metrics`.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--web.listen-address` | `:9352` | Address to listen on |
| `--web.telemetry-path` | `/metrics` | Path of the metrics endpoint |
| `--walg.path` | `wal-g` | Path to the `wal-g` binary |
| `--walg.config-path` | | Path to the WAL-G config file, passed to `wal-g` as `--config` (optional) |
| `--backup-list.scrape-interval` | `60s` | Interval between `backup-list` runs |
| `--binlog-list.scrape-interval` | `30s` | Interval between `binlog-list` runs |
| `--storage-check.scrape-interval` | `30s` | Interval between storage connectivity checks |

## WAL-G configuration

WAL-G has to be configured for the storage with the backups, either with a config file (`--walg.config-path`) or with
environment variables of the exporter process, for example:

```bash
export WALG_S3_PREFIX="s3://my-bucket/backups"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

## Prometheus configuration

```yaml
scrape_configs:
  - job_name: walg-mysql
    static_configs:
      - targets: ["localhost:9352"]
```

## Metrics

Backup metrics (one series per stored backup; series of deleted backups disappear on the next run):

| Metric | Labels | Description |
|--------|--------|-------------|
| `walg_mysql_backups_total` | | Number of backups in storage |
| `walg_mysql_backup_info` | `backup_name`, `hostname`, `is_permanent`, `binlog_start`, `binlog_end` | Metadata of a backup, the value is always 1 |
| `walg_mysql_backup_start_timestamp` | `backup_name` | Start time of the backup (Unix timestamp) |
| `walg_mysql_backup_finish_timestamp` | `backup_name` | Finish time of the backup (Unix timestamp) |
| `walg_mysql_backup_uncompressed_size_bytes` | `backup_name` | Uncompressed size of the backup |
| `walg_mysql_backup_compressed_size_bytes` | `backup_name` | Compressed size of the backup |
| `walg_mysql_backup_duration_seconds` | `backup_name` | Duration of the backup |
| `walg_mysql_backup_list_duration_seconds` | | Duration of the last `backup-list` run |

Binlog metrics:

| Metric | Description |
|--------|-------------|
| `walg_mysql_binlog_count` | Number of binlogs in storage |
| `walg_mysql_binlog_latest_timestamp` | Modification time of the most recent binlog (Unix timestamp) |
| `walg_mysql_binlog_total_size_bytes` | Total size of the binlogs in storage |
| `walg_mysql_binlog_coverage_seconds` | Time span covered by the binlogs in storage |
| `walg_mysql_binlog_list_duration_seconds` | Duration of the last `binlog-list` run |

Storage and exporter metrics:

| Metric | Labels | Description |
|--------|--------|-------------|
| `walg_mysql_storage_up` | | Storage connectivity (1 = reachable, 0 = not) |
| `walg_mysql_storage_latency_seconds` | | Latency of the last storage check |
| `walg_mysql_scrape_errors_total` | | Number of failed scrapes |
| `walg_mysql_errors_total` | `operation`, `error_type` | Number of WAL-G command errors |

## Example queries

```promql
# Hours since the last backup finished
(time() - max(walg_mysql_backup_finish_timestamp)) / 3600

# Seconds since the last binlog was archived
time() - walg_mysql_binlog_latest_timestamp

# Compression ratio per backup
walg_mysql_backup_compressed_size_bytes / walg_mysql_backup_uncompressed_size_bytes
```

Alerts:

```yaml
- alert: WalgNoRecentBackup
  expr: time() - max(walg_mysql_backup_finish_timestamp) > 25 * 3600
  for: 5m
- alert: WalgStorageUnreachable
  expr: walg_mysql_storage_up == 0
  for: 5m
```

## Troubleshooting

* No backup metrics: run the same `wal-g backup-list --detail --json` as the user of the exporter, and check that the
  WAL-G configuration is visible to the exporter process.
* `walg_mysql_storage_up` is 0: run `wal-g st check read` the same way.
* `walg_mysql_errors_total` grows: the `operation` label tells which `wal-g` command fails; its error is in the
  exporter log.
