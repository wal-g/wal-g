# WAL-G MySQL/MariaDB Prometheus Exporter

A production-ready Prometheus exporter for WAL-G backup and binlog monitoring for MySQL/MariaDB databases.

## Features

- **Comprehensive backup metrics**: Track backup timestamps, sizes, duration, and types
- **Binlog monitoring**: Monitor binlog count, latest timestamp, and total size
- **Storage health**: Monitor storage connectivity and latency
- **Error tracking**: Track WAL-G operation errors by type
- **Zero dependencies**: Single binary, no additional requirements
- **Graceful shutdown**: Proper signal handling for container environments
- **Health endpoint**: `/health` endpoint for orchestrator health checks

## Metrics

### Backup Metrics

- `walg_mysql_backups{backup_type}` - Number of backups by type (full/incremental)
- `walg_mysql_backup_start_timestamp{backup_name, backup_type, hostname, is_permanent, binlog_start, binlog_end}` - Backup start time (Unix timestamp)
- `walg_mysql_backup_finish_timestamp{backup_name, backup_type, hostname, is_permanent, binlog_start, binlog_end}` - Backup finish time (Unix timestamp)
- `walg_mysql_backup_compressed_size_bytes{backup_name, backup_type, hostname}` - Compressed backup size
- `walg_mysql_backup_uncompressed_size_bytes{backup_name, backup_type, hostname}` - Uncompressed backup size
- `walg_mysql_backup_duration_seconds{backup_name, backup_type}` - Backup duration

### Binlog Metrics

- `walg_mysql_binlog_count` - Number of binlogs in storage
- `walg_mysql_binlog_latest_timestamp` - Timestamp of latest binlog (Unix timestamp)
- `walg_mysql_binlog_total_size_bytes` - Total size of all binlogs in bytes

### Storage Health Metrics

- `walg_mysql_storage_alive` - Storage connectivity status (1 = alive, 0 = dead)
- `walg_mysql_storage_latency_seconds` - Storage operation latency

### Exporter Metrics

- `walg_mysql_scrape_duration_seconds` - Duration of the last scrape
- `walg_mysql_scrape_errors_total` - Total number of scrape errors
- `walg_mysql_errors_total{operation, error_type}` - Total errors by operation

## Installation

### Build from source

```bash
cd cmd/mysql/exporter
go build -o walg-mysql-exporter .
```

### Using Docker

```bash
docker build -t walg-mysql-exporter -f cmd/mysql/exporter/Dockerfile .
docker run -p 9352:9352 \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  -e WALG_S3_PREFIX=s3://my-bucket/backups \
  walg-mysql-exporter
```

## Configuration

The exporter is configured via command-line flags:

```bash
./walg-mysql-exporter [flags]
```

### Flags

- `--web.listen-address` - Address to listen on (default: `:9352`)
- `--web.telemetry-path` - Path for metrics endpoint (default: `/metrics`)
- `--walg.path` - Path to wal-g binary (default: `wal-g`)
- `--scrape.interval` - Interval between scrapes (default: `60s`)
- `--walg.config-path` - Path to wal-g config file (optional)

### Example

```bash
./walg-mysql-exporter \
  --web.listen-address=:9352 \
  --scrape.interval=30s \
  --walg.config-path=/etc/wal-g/config.json
```

## WAL-G Configuration

The exporter requires WAL-G to be properly configured. You can configure WAL-G using:

1. **Environment variables**:
```bash
export WALG_S3_PREFIX="s3://my-bucket/backups"
export AWS_ACCESS_KEY_ID="xxx"
export AWS_SECRET_ACCESS_KEY="xxx"
export WALG_MYSQL_DATASOURCE_NAME="user:pass@tcp(localhost:3306)/mysql"
```

2. **Config file** (use `--walg.config-path`):
```json
{
  "WALG_S3_PREFIX": "s3://my-bucket/backups",
  "AWS_ACCESS_KEY_ID": "xxx",
  "AWS_SECRET_ACCESS_KEY": "xxx",
  "WALG_MYSQL_DATASOURCE_NAME": "user:pass@tcp(localhost:3306)/mysql"
}
```

## Prometheus Configuration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'walg-mysql-exporter'
    static_configs:
      - targets: ['localhost:9352']
    scrape_interval: 60s
    metrics_path: /metrics
```

## Grafana Dashboard

### Example Queries

**Time since last backup**:
```promql
# In hours
(time() - walg_mysql_backup_finish_timestamp) / 3600
```

**Backup duration**:
```promql
walg_mysql_backup_duration_seconds
```

**Backup size compression ratio**:
```promql
walg_mysql_backup_compressed_size_bytes / walg_mysql_backup_uncompressed_size_bytes
```

**Latest backup age by type**:
```promql
(time() - max by (backup_type) (walg_mysql_backup_finish_timestamp)) / 3600
```

**Binlog lag** (time since last binlog):
```promql
(time() - walg_mysql_binlog_latest_timestamp) / 60
```

**Storage health**:
```promql
walg_mysql_storage_alive
```

**Error rate**:
```promql
rate(walg_mysql_errors_total[5m])
```

### Alerting Rules

```yaml
groups:
  - name: walg_mysql
    rules:
      - alert: WalgMySQLBackupTooOld
        expr: (time() - walg_mysql_backup_finish_timestamp) / 3600 > 25
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "MySQL backup is older than 25 hours"
          description: "Last backup {{ $labels.backup_name }} finished {{ $value | humanizeDuration }} ago"

      - alert: WalgMySQLStorageUnreachable
        expr: walg_mysql_storage_alive == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "WAL-G storage is unreachable"

      - alert: WalgMySQLNoBackups
        expr: walg_mysql_backups == 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "No MySQL backups found in storage"

      - alert: WalgMySQLScrapeErrors
        expr: rate(walg_mysql_scrape_errors_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WAL-G MySQL exporter is experiencing scrape errors"
```

## Docker Compose Example

```yaml
version: '3.8'

services:
  walg-mysql-exporter:
    build:
      context: .
      dockerfile: cmd/mysql/exporter/Dockerfile
    ports:
      - "9352:9352"
    environment:
      # AWS S3 Configuration
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - WALG_S3_PREFIX=s3://my-bucket/mysql-backups
      
      # MySQL Configuration
      - WALG_MYSQL_DATASOURCE_NAME=root:password@tcp(mysql:3306)/mysql
      
      # Optional: Compression/Encryption
      - WALG_COMPRESSION_METHOD=brotli
      - WALG_LIBSODIUM_KEY=${WALG_ENCRYPTION_KEY}
    command:
      - --scrape.interval=30s
      - --web.listen-address=:9352
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:9352/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: walg-mysql-exporter
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: walg-mysql-exporter
  template:
    metadata:
      labels:
        app: walg-mysql-exporter
    spec:
      containers:
      - name: exporter
        image: walg-mysql-exporter:latest
        ports:
        - containerPort: 9352
          name: metrics
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: walg-s3-credentials
              key: access-key-id
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: walg-s3-credentials
              key: secret-access-key
        - name: WALG_S3_PREFIX
          value: "s3://my-bucket/mysql-backups"
        - name: WALG_MYSQL_DATASOURCE_NAME
          valueFrom:
            secretKeyRef:
              name: mysql-credentials
              key: datasource-name
        args:
        - --scrape.interval=60s
        livenessProbe:
          httpGet:
            path: /health
            port: 9352
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 9352
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 200m
            memory: 256Mi
---
apiVersion: v1
kind: Service
metadata:
  name: walg-mysql-exporter
  namespace: monitoring
  labels:
    app: walg-mysql-exporter
spec:
  ports:
  - port: 9352
    targetPort: 9352
    name: metrics
  selector:
    app: walg-mysql-exporter
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: walg-mysql-exporter
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: walg-mysql-exporter
  endpoints:
  - port: metrics
    interval: 60s
    path: /metrics
```

## Troubleshooting

### No metrics appearing

1. **Check WAL-G is accessible**:
```bash
wal-g backup-list
```

2. **Check WAL-G configuration**:
```bash
# Verify environment variables or config file
env | grep WALG
```

3. **Check exporter logs**:
```bash
docker logs walg-mysql-exporter
```

### High scrape errors

- Check storage connectivity
- Verify WAL-G credentials are valid
- Increase scrape interval if storage is slow

### Storage health always shows 0

- Verify `wal-g st check read` command works manually
- Check storage credentials and permissions
- Ensure network access to storage backend

## Development

### Running locally

```bash
# Install dependencies
go mod download

# Run with local config
go run . \
  --walg.path=/usr/local/bin/wal-g \
  --scrape.interval=10s \
  --walg.config-path=./config.json
```

### Testing

```bash
# Build
go build -o walg-mysql-exporter .

# Test endpoints
curl http://localhost:9352/metrics
curl http://localhost:9352/health
```

## License

Same as WAL-G project license.

## Contributing

Contributions are welcome! Please ensure:
- Code follows project standards
- No hardcoded values
- Proper error handling
- Comprehensive logging
