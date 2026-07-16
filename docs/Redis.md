# WAL-G for Redis

**Interface of Redis features is currently unstable**

You can use wal-g as a tool for making encrypted, compressed Redis backups and push/fetch them to/from storage without saving it on your filesystem.

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

### ``backup-push``

Creates and uploads a Redis backup. The `--type` (`-t`) flag selects its payload:

- `rdb` (default): an RDB stream produced by `WALG_STREAM_CREATE_COMMAND`.
- `aof`: Redis AOF files.
- `rdb_ts`, `aof_ts`, and `ts`: reserved for Valkey tiered-storage backups and are unavailable until tiered-storage support is enabled.

```bash
wal-g redis backup-push --type rdb
wal-g redis backup-push --type aof
```

The legacy `rdb-backup-push` and `aof-backup-push` commands have been removed.

### `backup-list`

Lists currently available backups in storage.

```bash
wal-g backup-list
```

### `backup-fetch`

Fetches a backup by name. Use `--type rdb` (the default) to restore through
`WALG_STREAM_RESTORE_COMMAND`; use `--type aof --redis-version <version>` for
an AOF restore. `--redis-version` is required for AOF compatibility checks.

```bash
wal-g redis backup-fetch example_backup --type rdb
wal-g redis backup-fetch example_backup --type aof --redis-version 7.2
```

The legacy `rdb-backup-fetch` and `aof-backup-fetch` commands have been removed.

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