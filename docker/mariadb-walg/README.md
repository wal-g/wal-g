# MariaDB + WAL-G Docker Image

Production-ready Docker image combining MariaDB with WAL-G for enterprise-grade backup and recovery.

## Features

- ✅ **Automatic Restore on Startup**: Restores from backup if datadir is empty
- ✅ **Point-in-Time Recovery (PITR)**: Applies binlogs to specific timestamp
- ✅ **Graceful Shutdown**: Proper signal handling with tini
- ✅ **Permission Management**: Automatic ownership fixing
- ✅ **Production Ready**: Mission-critical reliability
- ✅ **Docker & Kubernetes**: Works in both environments

## Quick Start

### Docker Compose

```yaml
version: '3.8'

services:
  mariadb:
    image: mariadb-walg:latest
    environment:
      # Standard MariaDB vars
      MYSQL_ROOT_PASSWORD: secretpass
      MYSQL_DATABASE: myapp
      
      # WAL-G storage config
      AWS_ACCESS_KEY_ID: your_key
      AWS_SECRET_ACCESS_KEY: your_secret
      AWS_REGION: us-east-1
      WALG_S3_PREFIX: s3://bucket/path
      
      # Restore config (optional)
      WALG_RESTORE_FROM_BACKUP: LATEST  # or specific backup name
      
      # PITR config (optional)
      WALG_PITR_UNTIL: "2024-01-15 10:30:00"
      WALG_PITR_SINCE: LATEST
      
      # Binlog replay command
      WALG_MYSQL_BINLOG_REPLAY_COMMAND: /usr/local/bin/binlog-replay-helper.sh
      WALG_MYSQL_BINLOG_DST: /tmp/binlogs
    volumes:
      - mariadb_data:/var/lib/mysql
    ports:
      - "3306:3306"

volumes:
  mariadb_data:
```

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mariadb-walg
spec:
  initContainers:
  - name: restore
    image: mariadb-walg:latest
    command: ["/bin/bash", "-c"]
    args:
    - |
      if [ ! -d /var/lib/mysql/mysql ]; then
        /usr/local/bin/wal-g backup-fetch LATEST /var/lib/mysql
        chown -R 999:999 /var/lib/mysql
      fi
    env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: s3-credentials
          key: access-key-id
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: s3-credentials
          key: secret-access-key
    - name: WALG_S3_PREFIX
      value: s3://bucket/path
    volumeMounts:
    - name: data
      mountPath: /var/lib/mysql
    securityContext:
      runAsUser: 999   # mysql user
      runAsGroup: 999
      fsGroup: 999
  
  containers:
  - name: mariadb
    image: mariadb:11.0
    env:
    - name: MYSQL_ROOT_PASSWORD
      valueFrom:
        secretKeyRef:
          name: mysql-credentials
          key: root-password
    volumeMounts:
    - name: data
      mountPath: /var/lib/mysql
  
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: mariadb-pvc
```

## Environment Variables

### Required for Restore

| Variable | Description | Example |
|----------|-------------|---------|
| `WALG_RESTORE_FROM_BACKUP` | Backup name to restore | `LATEST` or `base_000000010000000000000002` |
| `WALG_S3_PREFIX` | S3 path prefix | `s3://bucket/path` |
| `AWS_ACCESS_KEY_ID` | AWS credentials | `AKIAIOSFODNN7EXAMPLE` |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |

### Optional for PITR

| Variable | Description | Default |
|----------|-------------|---------|
| `WALG_PITR_UNTIL` | Restore up to this time | (none) |
| `WALG_PITR_SINCE` | Start from this backup | `LATEST` |
| `WALG_MYSQL_BINLOG_REPLAY_COMMAND` | Replay script | `/usr/local/bin/binlog-replay-helper.sh` |
| `WALG_MYSQL_BINLOG_DST` | Temp binlog dir | `/tmp/binlogs` |

## Operational Modes

### Mode 1: Fresh Install (No Restore)

```bash
docker run -e MYSQL_ROOT_PASSWORD=pass mariadb-walg:latest
```

- Datadir empty + no `WALG_RESTORE_FROM_BACKUP`
- MariaDB initializes normally

### Mode 2: Restore from Backup

```bash
docker run \
  -e MYSQL_ROOT_PASSWORD=pass \
  -e WALG_RESTORE_FROM_BACKUP=LATEST \
  -e WALG_S3_PREFIX=s3://bucket/backups \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=yyy \
  mariadb-walg:latest
```

- Detects empty datadir
- Fetches backup from S3
- Fixes permissions
- Starts MariaDB

### Mode 3: Point-in-Time Recovery (PITR)

```bash
docker run \
  -e MYSQL_ROOT_PASSWORD=pass \
  -e WALG_RESTORE_FROM_BACKUP=LATEST \
  -e WALG_PITR_UNTIL="2024-01-15 10:30:00" \
  -e WALG_S3_PREFIX=s3://bucket/backups \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=yyy \
  -e WALG_MYSQL_BINLOG_REPLAY_COMMAND=/usr/local/bin/binlog-replay-helper.sh \
  -e WALG_MYSQL_BINLOG_DST=/tmp/binlogs \
  mariadb-walg:latest
```

- Restores backup
- Starts MariaDB
- Applies binlogs up to specified time
- Ready for use

## Build Instructions

```bash
# Build wal-g for linux
cd /path/to/wallg
make linux-build

# Copy to docker directory
cp main/mysql/wal-g-linux docker/mariadb-walg/wal-g

# Build image
cd docker/mariadb-walg
docker build -t mariadb-walg:latest .
```

## Testing

See `test-e2e.sh` for comprehensive end-to-end testing.

```bash
./test-e2e.sh
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  tini (PID 1)                                               │
│  ├─ docker-entrypoint-walg.sh                              │
│     ├─ Phase 1: wal-g backup-fetch (if needed)            │
│     ├─ Phase 2: docker-entrypoint.sh mariadbd &           │
│     ├─ Phase 3: wal-g binlog-replay (if PITR)             │
│     └─ Phase 4: wait + signal handling                    │
└─────────────────────────────────────────────────────────────┘
```

## Production Considerations

### Security

- Store credentials in secrets (Kubernetes) or `.env` files (Docker)
- Use IAM roles when running in AWS (no keys needed)
- Run as non-root user in K8s (use securityContext)

### Performance

- Use local SSD volumes for `/var/lib/mysql`
- Configure `WALG_DOWNLOAD_CONCURRENCY` for faster restores
- Use `WALG_COMPRESSION_METHOD=lz4` for speed

### Monitoring

- Check container logs: `docker logs -f mariadb`
- Verify restore: Look for "✅ Backup restaurado exitosamente"
- Verify PITR: Look for "✅ PITR completado"

## Troubleshooting

### ERROR 1045: Access denied

**Cause**: Password mismatch after restore.

**Solution**: Use the original backup's root password, not a new one.

### Permission denied on datadir

**Cause**: Incorrect ownership after restore.

**Solution**: The wrapper automatically runs `chown -R mysql:mysql`. If in K8s, ensure `securityContext.fsGroup: 999`.

### MariaDB won't start after restore

**Cause**: Corrupted datadir or incomplete restore.

**Solution**: 
1. Check logs: `docker logs mariadb`
2. Verify backup integrity
3. Try a different backup: `WALG_RESTORE_FROM_BACKUP=base_xxx`

## License

See main WAL-G LICENSE.md
