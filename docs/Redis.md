# WAL-G for Redis and Valkey

**Interface of Redis features is currently unstable**

You can use wal-g as a tool for making encrypted, compressed Redis or Valkey backups and push/fetch them to/from storage without saving it on your filesystem. Valkey uses the Redis-compatible WAL-G command surface.

Configuration
-------------

* `WALG_STREAM_CREATE_COMMAND`

Command to create Redis backup, should return backup as single stream to STDOUT. Required for backup procedure.

* `WALG_STREAM_RESTORE_COMMAND`

Command to unpack Redis backup, should take backup (created by `WALG_STREAM_CREATE_COMMAND`)
to STDIN. Required for restore procedure.

* `WALG_REDIS_USERNAME`

Username for 'redis-cli' command. Required for interacting with server if you have password.

* `WALG_REDIS_PASSWORD`

Password for 'redis-cli' command. Required for backup archiving procedure if you have password.

Usage
-----

WAL-G redis extension currently supports these commands:

### ``copy``

Copies one backup or all backups between storage configurations without transforming payload objects:

```bash
wal-g copy --from=config_from.json --to=config_to.json --backup-name=LATEST
```

Redis/Valkey backups are standalone, so this command does not copy unrelated archive history. Repeating it skips immutable objects already present at the destination.

### ``backup-push``

Creates and uploads a Redis or Valkey backup. The `--type` (`-t`) flag selects its payload:

- `rdb` (default): an RDB stream produced by `WALG_STREAM_CREATE_COMMAND`.
- `aof`: Redis AOF files.
- `rdb_ts`: an RDB stream and a Valkey tiered-storage tree.
- `aof_ts`: AOF files and a Valkey tiered-storage tree.
- `ts`: a standalone Valkey tiered-storage tree.

```bash
wal-g redis backup-push --type rdb
wal-g redis backup-push --type aof
wal-g redis backup-push --type rdb_ts --ts-backup /var/lib/redis/ext/frozen-backup --ts-backup-id backup-id
wal-g redis backup-push --type ts --ts-backup /var/lib/redis/ext/frozen-backup --ts-backup-id backup-id
```

`--ts-backup` must name a non-empty frozen directory for `rdb_ts`, `aof_ts`, and
`ts`; it is rejected for plain `rdb` and `aof`. `--ts-backup-id` is valid only
for tiered-storage types and is persisted with the TS sentinel. WAL-G only moves
files: an external orchestrator must create the frozen tree before upload and
load it after restore. WAL-G never invokes Valkey `EXTERNAL_DATA` commands.

The TS pin directory is controlled by `WALG_REDIS_TS_PIN_FOLDER` (default
`/var/lib/redis/ext/wal-g-pin/`). It must be on the same filesystem as
`--ts-backup`, because WAL-G protects each source file with a hard link and an
open file descriptor for the duration of the upload.

For `rdb_ts` and `aof_ts`, backup is all-or-nothing: WAL-G uploads both payloads
in parallel, but publishes neither sentinel until both finish successfully. If
either fails, WAL-G removes the entire backup prefix and exits with status 1.
Standalone `ts` backups use a top-level `ts_<timestamp>` name; attached TS data
is stored below `<main-backup>/ts_data/` and is removed together with its parent.

The legacy `rdb-backup-push` and `aof-backup-push` commands have been removed.

### `backup-list` and `backup-info`

Lists currently available backups in storage. `backup-list --detail` and
`backup-info <name>` expose `has_ts`, TS backup ID, path, size, and file count
for attached TS data. The internal attached TS sentinel is not discovered as a
separate backup row; standalone `ts_<timestamp>` backups are listed normally.

```bash
wal-g redis backup-list --detail
wal-g redis backup-info stream_20260721T120000Z
```

### `backup-fetch`

Fetches a backup by name. Use `--type rdb` (the default) to restore through
`WALG_STREAM_RESTORE_COMMAND`; use `--type aof --redis-version <version>` for
an AOF restore. `--redis-version` is required for `aof` and `aof_ts`.

For `rdb_ts`, `aof_ts`, and `ts`, `--ts-backup` is required and receives the
downloaded TS tree. Combined restores fetch the main payload first and then the
TS tree; a TS download failure fails the complete restore. The target must be
empty because TS restore follows the normal clean policy.

```bash
wal-g redis backup-fetch example_backup --type rdb
wal-g redis backup-fetch example_backup --type aof --redis-version 7.2
wal-g redis backup-fetch stream_20260721T120000Z --type rdb_ts --ts-backup /var/lib/redis/ext/restore
wal-g redis backup-fetch ts_20260721T120000Z --type ts --ts-backup /var/lib/redis/ext/restore
```

The legacy `rdb-backup-fetch` and `aof-backup-fetch` commands have been removed.

> **Deployment requirement:** the command removals and tiered-storage interface are
> a breaking package change. Deploy WAL-G in lock-step with the orchestration
> that creates frozen TS backups and invokes the new command surface.

### `delete`

Deletes backups from storage, keeps N backups.

User should specify time or date to keep.

Dry-run remove retain all after specific date
```bash
wal-g delete --retain-after 2020-10-28T12:11:10+03:00
```

or

Dry-run keep 10 last backups
```bash
wal-g delete --retain-count 10
```

or

Dry-run keep at least 10 backups, and retain all after specific date
```bash
wal-g delete --retain-count 10 --retain-after 2020-10-28T12:11:10+03:00
```


Perform delete
```bash
wal-g delete --retain-count 10 --retain-after 2020-10-28T12:11:10+03:00 --confirm
```

Typical configurations
-----

### Full backup/restore only

Here's typical wal-g configuration for that case:
```bash
WALG_STREAM_CREATE_COMMAND:  'redis_cli --rdb /dev/stdout'
WALG_STREAM_RESTORE_COMMAND: 'cat > /var/lib/redis/dump.rdb'
```

### Why we made redis_cli.sh
redis-cli fails with error when redis version >= 6.2, so we made this workaround

If you use redis >= 6.2, use [redis_cli.sh](https://github.com/wal-g/wal-g/blob/master/redis_cli.sh) and replace redis-cli in WALG_STREAM_CREATE_COMMAND
