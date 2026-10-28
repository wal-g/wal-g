# WAL-G MySQL/MariaDB Prometheus exporter

A Prometheus exporter for monitoring WAL-G backups and binlogs of MySQL and MariaDB. It does not talk to the
database: it runs the `wal-g` binary (`backup-list --detail --json`, `binlog-list --json`, `st check read`) and exposes
the result as metrics.

## Build and run

The exporter is a separate Go module:

```bash
cd cmd/mysql/exporter
go build -o walg-mysql-exporter .

./walg-mysql-exporter --walg.path=/usr/local/bin/wal-g --walg.config-path=/etc/wal-g/config.json
```

Metrics are served on `http://<listen-address>/metrics`, `/health` is a liveness probe and `/ready` is a readiness
probe (it succeeds after the first successful storage check and WAL-G scrape).

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--web.listen-address` | `:9352` | Address to listen on |
| `--web.telemetry-path` | `/metrics` | Path of the metrics endpoint |
| `--walg.path` | `wal-g` | Path to the `wal-g` binary |
| `--walg.config-path` | | Path to the WAL-G config file, passed to `wal-g` as `--config` (optional) |
| `--walg.scrape.interval` | `60s` | Interval between `backup-list` and `binlog-list` runs |
| `--storage.scrape.interval` | `30s` | Interval between storage connectivity checks |
| `--database` | `mysql` | `mysql` or `mariadb`: selects the prefix of the metric names, `walg_mysql` or `walg_mariadb` |

### Metric name prefix

The exporter serves both databases and only the prefix of the metric names depends on `--database`: `walg_mysql` for
`mysql` (the default) and `walg_mariadb` for `mariadb`. Any other value is rejected. The names below are written for the
default.

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

Backup metrics, one series per finished backup (series of deleted backups disappear on the next scrape):

| Metric | Labels | Description |
|--------|--------|-------------|
| `walg_mysql_backups` | `backup_type` | Number of backups in storage |
| `walg_mysql_backup_start_timestamp` | `backup_name` | Start time of the backup (Unix timestamp) |
| `walg_mysql_backup_finish_timestamp` | `backup_name` | Finish time of the backup (Unix timestamp) |
| `walg_mysql_backup_uncompressed_size_bytes` | `backup_name` | Uncompressed size of the backup |
| `walg_mysql_backup_compressed_size_bytes` | `backup_name` | Compressed size of the backup |
| `walg_mysql_backup_duration_seconds` | `backup_name` | Duration of the backup |

`backup_type` is always `full`: `wal-g backup-list` does not tell full and incremental backups apart.

Binlog metrics:

| Metric | Description |
|--------|-------------|
| `walg_mysql_binlog_count` | Number of binlogs in storage |
| `walg_mysql_binlog_latest_timestamp` | Modification time of the most recent binlog (Unix timestamp) |
| `walg_mysql_binlog_oldest_timestamp` | Modification time of the oldest binlog (Unix timestamp) |
| `walg_mysql_binlog_total_size_bytes` | Total size of the binlogs in storage |
| `walg_mysql_rpo_seconds` | Seconds since the most recent binlog was archived: the data you can lose on a failure |
| `walg_mysql_binlog_gap_seconds` | Oldest binlog minus the finish of the latest backup. Positive means the binlogs do not reach back to the latest backup, so point-in-time recovery has a hole |

Storage and exporter metrics:

| Metric | Labels | Description |
|--------|--------|-------------|
| `walg_mysql_storage_up` | | Storage connectivity (1 = reachable, 0 = not) |
| `walg_mysql_storage_latency_seconds` | | Latency of the last storage check |
| `walg_mysql_storage_scrape_duration_seconds` | | Duration of the last storage check |
| `walg_mysql_walg_scrape_duration_seconds` | | Duration of the last WAL-G scrape (`backup-list` and `binlog-list`) |
| `walg_mysql_backup_scrape_duration_seconds` | | Duration of the last `backup-list` run |
| `walg_mysql_binlog_scrape_duration_seconds` | | Duration of the last `binlog-list` run |
| `walg_mysql_scrape_errors_total` | | Number of failed WAL-G scrapes |
| `walg_mysql_errors_total` | `operation`, `error_type` | Number of errors by operation (`backup-list`, `binlog-list`, `storage-check`) |

## Example queries and alerts

```promql
# Hours since the last backup finished
(time() - max(walg_mysql_backup_finish_timestamp)) / 3600

# How much data can be lost now, in seconds
walg_mysql_rpo_seconds
```

```yaml
- alert: WalgNoRecentBackup
  expr: time() - max(walg_mysql_backup_finish_timestamp) > 25 * 3600
  for: 5m
- alert: WalgBinlogGap
  expr: walg_mysql_binlog_gap_seconds > 0
  for: 5m
- alert: WalgStorageUnreachable
  expr: walg_mysql_storage_up == 0
  for: 5m
```

## Troubleshooting

* No backup or binlog metrics: run the same `wal-g backup-list --detail --json` and `wal-g binlog-list --json` as the
  user of the exporter, and check that the WAL-G configuration is visible to the exporter process.
* `walg_mysql_storage_up` is 0: run `wal-g st check read` the same way.
* `walg_mysql_errors_total` grows: the `operation` label tells which `wal-g` command fails; the error is in the
  exporter log.
