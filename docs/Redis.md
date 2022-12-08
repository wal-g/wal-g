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

* `WALG_REDIS_PASSWORD`

Password for 'redis-cli' command. Required for backup archiving procedure if you have password.

Usage
-----

WAL-G redis extension currently supports these commands:

### ``backup-push``

Creates new backup and send it to storage.

Runs `WALG_STREAM_CREATE_COMMAND` to create backup.

```bash
wal-g backup-push
```

### `backup-list`

Lists currently available backups in storage.

```bash
wal-g backup-list
```

### `backup-fetch`

Fetches backup from storage and restores passes data to `WALG_STREAM_RESTORE_COMMAND` to restore backup.

User should specify the name of the backup to fetch.

```bash
wal-g backup-fetch example_backup
```

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