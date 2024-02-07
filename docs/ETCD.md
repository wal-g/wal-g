# WAL-G for ETCD

**Work in progress**

You can use wal-g as a tool for encrypting, compressing ETCD backups and push/fetch them to/from storage without saving it on your filesystem.

Configuration
-------------

* `WALG_STREAM_CREATE_COMMAND`

Command to create ETCD backup, should return backup as single stream to STDOUT. Required for backup procedure.

* `WALG_STREAM_RESTORE_COMMAND`

Command to unpack ETCD backup, should take backup (created by `WALG_STREAM_CREATE_COMMAND`)
to STDIN. Required for restore procedure.

* `WALG_ETCD_DATA_DIR`

Route to ETCD data-dir. Stored in env variable ETCD_DATA_DIR or specified in --data-dir flag when running etcd command. Required for wal archiving procedure.

* `WALG_ETCD_WAL_DIR`

Route to ETCD wal-dir. Use it in case you changed wal directory when configuring cluster.

Usage
-----

WAL-G etcd extension currently supports these commands:

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
wal-g backup-fetch backup_name
```

WAL-G can also fetch the latest backup using:

```bash
wal-g backup-fetch LATEST
```

### `wal-push`

Get all wal files from etcd data directory and send to storage. Data directory must be stored in `WALG_ETCD_DATA_DIR`. 
If you set `WALG_ETCD_WAL_DIR` then this directory wil be used.

On second run sends only complete wal files which are not yet in storage.

```bash
wal-g wal-push
```

### `wal-fetch`

Fetches wal files from storage and send it to specified dest-dir. Only wals that were created after specified backup will be fetched.

By default command use latest created backup.

```bash
wal-g wal-fetch dest-dir --since LATEST
```

### `delete`

Deletes backups from storage.

Dry-run remove retain all after specific backup including specified backup
```bash
wal-g delete before backup_name
```

or

Dry-run keep 3 last backups
```bash
wal-g delete retain 3
```

or

Dry-run delete all backups from storage
```bash
wal-g delete everything
```


In order to perform delete use --confirm flag
```bash
wal-g delete everything --confirm
```

Typical configurations
-----

```bash
WALG_STREAM_CREATE_COMMAND: 'TMP_DIR=$(mktemp) && etcdctl snapshot save $TMP_DIR > /dev/null && cat < $TMP_DIR'
WALG_STREAM_RESTORE_COMMAND: 'TMP_DIR=$(mktemp) && cat > $TMP_DIR && etcdctl snapshot restore $TMP_DIR --data-dir $ETCD_RESTORE_DATA_DIR'
WALG_ETCD_DATA_DIR: '/tmp/etcd/cluster'
```