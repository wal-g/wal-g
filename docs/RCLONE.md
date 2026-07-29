# Rclone Storage Support

WAL-G supports Rclone as a storage backend, enabling backups to 40+ cloud storage providers.

## Quick Start

```bash
# Install Rclone
curl https://rclone.org/install.sh | sudo bash

# Configure a remote (e.g. named "myremote")
rclone config

# Set WAL-G environment variables
# WALG_RCLONE_PREFIX format: <scheme>://<bucket>/<path>
# The scheme is used only to extract bucket and path; RCLONE_REMOTE is the actual remote name.
export WALG_RCLONE_PREFIX="myremote://mybucket/backups"
export RCLONE_REMOTE="myremote"

# Run backup
wal-g backup-push
```

## Configuration

### Required Variables

- `WALG_RCLONE_PREFIX`: Storage prefix in the form `<name>://<bucket>/<path>`. The scheme is used to extract the bucket and path; the actual Rclone remote is set via `RCLONE_REMOTE`.
- `RCLONE_REMOTE`: Name of the configured Rclone remote.

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RCLONE_CONFIG_PATH` | `~/.config/rclone/rclone.conf` | Rclone config file path |
| `RCLONE_BINARY_PATH` | `rclone` | Rclone binary path |
| `RCLONE_TRANSFERS` | `4` | Parallel transfer count |
| `RCLONE_BUFFER_SIZE` | `16777216` | Buffer size in bytes |
| `RCLONE_TIMEOUT` | `300` | Operation timeout in seconds |
| `RCLONE_RETRIES` | `3` | Retry count |
| `RCLONE_LOW_LEVEL_RETRIES` | `10` | Low-level retry count |
| `RCLONE_S3_CHUNK_SIZE` | - | S3 multipart chunk size in bytes (e.g. `67108864` for 64MB). Only passed to rclone when set. |
| `RCLONE_UPLOAD_CONCURRENCY` | - | S3 upload concurrency. Only passed to rclone when set. |
| `RCLONE_EXTRA_ARGS` | - | Additional Rclone arguments |

## Supported Providers

Rclone supports Amazon S3, Google Cloud Storage, Azure, Backblaze B2, Dropbox, Google Drive, Wasabi, and 40+ more providers.

See [Rclone providers](https://rclone.org/#providers) for the complete list.

## Performance Tuning

```bash
export RCLONE_TRANSFERS=16
export RCLONE_BUFFER_SIZE=33554432        # 32MB
export RCLONE_S3_CHUNK_SIZE=134217728     # 128MB
export RCLONE_UPLOAD_CONCURRENCY=8
export RCLONE_EXTRA_ARGS="--fast-list --checkers 16"
```

## Examples

### Amazon S3

```bash
rclone config
export WALG_RCLONE_PREFIX="s3://mybucket/backups"
export RCLONE_REMOTE="s3"
export RCLONE_TRANSFERS=8
wal-g backup-push
```

### Backblaze B2

```bash
rclone config
export WALG_RCLONE_PREFIX="b2://mybucket/mysql"
export RCLONE_REMOTE="b2"
wal-g backup-push
```

### Dropbox

```bash
rclone config
export WALG_RCLONE_PREFIX="dropbox://Apps/walg"
export RCLONE_REMOTE="dropbox"
wal-g backup-push
```
