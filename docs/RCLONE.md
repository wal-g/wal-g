# Rclone storage

WAL-G can store backups through [Rclone](https://rclone.org/), which lets it use any storage provider
that Rclone supports (see the [list of providers](https://rclone.org/#providers)).

## How it works

WAL-G runs the `rclone` binary for every storage operation (`lsjson`, `cat`, `rcat`, `copyto`, `deletefile`, `about`).
This means:

* `rclone` must be installed on every host that runs WAL-G, and be reachable via `PATH` or `RCLONE_BINARY_PATH`.
  See the [Rclone install guide](https://rclone.org/install/).
* Each operation starts a separate Rclone process, which adds some overhead compared to the built-in storages.
* The Rclone process inherits the environment of WAL-G, so credentials and remotes provided through the
  environment keep working.

## Quick start

1. Define a remote with `rclone config`, or through `RCLONE_CONFIG_<REMOTE>_*` environment variables.
2. Point WAL-G at it:

```bash
export WALG_RCLONE_PREFIX="rclone://mybucket/backups"
export RCLONE_REMOTE="myremote"

wal-g backup-push ...
```

`WALG_RCLONE_PREFIX` is parsed as a URL: the part after `://` is used as `<RCLONE_REMOTE>:<host>/<path>`, so the
example above addresses `myremote:mybucket/backups`. The scheme itself is ignored.

## Variables

| Variable                    | Required | Default                    | Description                                             |
|-----------------------------|----------|----------------------------|---------------------------------------------------------|
| `WALG_RCLONE_PREFIX`        | yes      |                            | Storage prefix, e.g. `rclone://bucket/path`             |
| `RCLONE_REMOTE`             | yes      |                            | Name of the Rclone remote                               |
| `RCLONE_CONFIG_PATH`        | no       | Rclone's default location  | Path to the Rclone config file                          |
| `RCLONE_BINARY_PATH`        | no       | `rclone`                   | Path to the Rclone binary                               |
| `RCLONE_TRANSFERS`          | no       | `4`                        | Number of parallel transfers                            |
| `RCLONE_BUFFER_SIZE`        | no       | `16777216`                 | Buffer size in bytes                                    |
| `RCLONE_TIMEOUT`            | no       | `300`                      | Operation timeout in seconds                            |
| `RCLONE_RETRIES`            | no       | `3`                        | Number of retries                                       |
| `RCLONE_LOW_LEVEL_RETRIES`  | no       | `10`                       | Number of low-level retries                             |
| `RCLONE_S3_CHUNK_SIZE`      | no       | `67108864`                 | S3 multipart chunk size in bytes (S3-type remotes only) |
| `RCLONE_UPLOAD_CONCURRENCY` | no       | `4`                        | Concurrent S3 upload parts (S3-type remotes only)       |
| `RCLONE_EXTRA_ARGS`         | no       |                            | Extra arguments for every Rclone command, space-separated |

## Examples

### S3-compatible remote defined in the Rclone config file

```bash
export WALG_RCLONE_PREFIX="rclone://mybucket/backups"
export RCLONE_REMOTE="s3remote"
export RCLONE_CONFIG_PATH="/etc/rclone/rclone.conf"
export RCLONE_TRANSFERS=8
```

### Remote defined only through environment variables

```bash
export RCLONE_CONFIG_MYREMOTE_TYPE=s3
export RCLONE_CONFIG_MYREMOTE_PROVIDER=Minio
export RCLONE_CONFIG_MYREMOTE_ENDPOINT=http://minio:9000
export RCLONE_CONFIG_MYREMOTE_ACCESS_KEY_ID=...
export RCLONE_CONFIG_MYREMOTE_SECRET_ACCESS_KEY=...

export WALG_RCLONE_PREFIX="rclone://mybucket/backups"
export RCLONE_REMOTE="myremote"
```

### Passing additional Rclone flags

```bash
export RCLONE_EXTRA_ARGS="--fast-list --checkers 16"
```
