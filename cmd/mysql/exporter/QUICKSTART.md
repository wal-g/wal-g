# WAL-G MySQL Exporter - Quick Start Guide

Get started with the WAL-G MySQL/MariaDB Prometheus exporter in 5 minutes.

## Prerequisites

- WAL-G installed and configured
- Go 1.21+ (for building from source)
- Access to MySQL/MariaDB backups in storage (S3, Azure, GCS, etc.)

## Quick Start

### 1. Build the Exporter

```bash
cd cmd/mysql/exporter
make build
```

This creates the `walg-mysql-exporter` binary.

### 2. Configure WAL-G

Set up your environment variables:

```bash
export WALG_S3_PREFIX="s3://my-bucket/mysql-backups"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export WALG_MYSQL_DATASOURCE_NAME="user:pass@tcp(localhost:3306)/mysql"
```

Or use a config file:

```bash
cp config.example.json config.json
# Edit config.json with your credentials
```

### 3. Start the Exporter

```bash
./walg-mysql-exporter \
  --web.listen-address=:9352 \
  --scrape.interval=60s
```

Or with config file:

```bash
./walg-mysql-exporter \
  --web.listen-address=:9352 \
  --scrape.interval=60s \
  --walg.config-path=./config.json
```

### 4. Verify it's Working

```bash
# Check health
curl http://localhost:9352/health

# View metrics
curl http://localhost:9352/metrics | grep walg_mysql

# Open in browser
open http://localhost:9352
```

## Docker Quick Start

### 1. Using Docker Compose

```bash
# Copy environment file
cp env.example .env

# Edit .env with your credentials
nano .env

# Start everything (exporter + Prometheus + Grafana)
docker-compose up -d

# View logs
docker-compose logs -f walg-mysql-exporter
```

### 2. Using Docker Run

```bash
docker run -d \
  --name walg-mysql-exporter \
  -p 9352:9352 \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  -e WALG_S3_PREFIX=s3://my-bucket/backups \
  -e WALG_MYSQL_DATASOURCE_NAME="user:pass@tcp(mysql:3306)/mysql" \
  walg-mysql-exporter:latest
```

## Kubernetes Quick Start

```bash
# Apply the deployment
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: walg-credentials
  namespace: monitoring
type: Opaque
stringData:
  access-key-id: "your-access-key"
  secret-access-key: "your-secret-key"
  datasource-name: "user:pass@tcp(mysql:3306)/mysql"
---
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
              name: walg-credentials
              key: access-key-id
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: walg-credentials
              key: secret-access-key
        - name: WALG_S3_PREFIX
          value: "s3://my-bucket/mysql-backups"
        - name: WALG_MYSQL_DATASOURCE_NAME
          valueFrom:
            secretKeyRef:
              name: walg-credentials
              key: datasource-name
---
apiVersion: v1
kind: Service
metadata:
  name: walg-mysql-exporter
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: walg-mysql-exporter
  ports:
  - port: 9352
    name: metrics
EOF
```

## Prometheus Configuration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'walg-mysql'
    static_configs:
      - targets: ['localhost:9352']
    scrape_interval: 60s
```

Reload Prometheus:

```bash
curl -X POST http://localhost:9090/-/reload
```

## Verify Metrics in Prometheus

1. Open Prometheus: http://localhost:9090
2. Go to "Status" → "Targets"
3. Verify "walg-mysql" target is UP
4. Go to "Graph"
5. Try query: `walg_mysql_backups`

## Useful Queries

### Time since last backup (hours)
```promql
(time() - walg_mysql_backup_finish_timestamp) / 3600
```

### Latest backup size (GB)
```promql
max(walg_mysql_backup_compressed_size_bytes) / 1024 / 1024 / 1024
```

### Backup duration (minutes)
```promql
walg_mysql_backup_duration_seconds / 60
```

### Storage health
```promql
walg_mysql_storage_alive
```

## Grafana Dashboard

### Import Dashboard

1. Open Grafana: http://localhost:3000
2. Login (default: admin/admin)
3. Go to "+" → "Import"
4. Use queries from README.md to create panels

### Key Panels

**Backup Age**:
- Query: `(time() - walg_mysql_backup_finish_timestamp) / 3600`
- Panel: Gauge
- Thresholds: Green < 12h, Yellow < 24h, Red > 24h

**Backup Size Trend**:
- Query: `walg_mysql_backup_compressed_size_bytes`
- Panel: Time Series
- Unit: Bytes (SI)

**Storage Health**:
- Query: `walg_mysql_storage_alive`
- Panel: Stat
- Mapping: 0 = Down (Red), 1 = Up (Green)

## Alerting

### Basic Alert Rules

Create `alert_rules.yml`:

```yaml
groups:
  - name: walg_mysql_alerts
    rules:
      - alert: BackupTooOld
        expr: (time() - walg_mysql_backup_finish_timestamp) / 3600 > 25
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "MySQL backup is too old"
          
      - alert: StorageDown
        expr: walg_mysql_storage_alive == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "WAL-G storage unreachable"
```

## Troubleshooting

### Exporter won't start

```bash
# Check if WAL-G is accessible
which wal-g

# Test WAL-G manually
wal-g backup-list

# Check logs
./walg-mysql-exporter 2>&1 | tee exporter.log
```

### No metrics appearing

```bash
# Test backup-list command
wal-g backup-list --detail --json

# Test storage connectivity
wal-g st check read

# Check exporter logs for errors
```

### Metrics are zero

- Ensure backups exist in storage
- Check WAL-G configuration
- Verify credentials are correct

## Next Steps

1. Set up alerting rules
2. Create Grafana dashboards
3. Configure notification channels
4. Document runbooks for alerts
5. Set up log aggregation

## Support

- **Full Documentation**: [README.md](README.md)
- **WAL-G Docs**: [docs/MySQL.md](../../../docs/MySQL.md)
- **Issues**: [GitHub Issues](https://github.com/wal-g/wal-g/issues)

---
