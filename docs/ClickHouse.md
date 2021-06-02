# WAL-G for ClickHouse

**Work in progress**

Configuration
-------------

* `WALG_STREAM_CREATE_COMMAND`

Command to create ClickHouse backup, should return backup as single stream to STDOUT. Required for backup procedure.

Usage
-----

### ``backup-push``

Creates new backup and sends it to storage. 

Runs `WALG_STREAM_CREATE_COMMAND` to create backup.

```bash
wal-g backup-push
```

Typical configurations
-----

### Full backup only - using with `clickhouse-backup`


```bash                                                                                                                                   
 WALG_STREAM_CREATE_COMMAND="TMP_BACKUP_NAME=tmp_$(date +"%Y-%m-%dT%H-%M-%S") && sudo clickhouse-backup create $TMP_BACKUP_NAME 1>/dev/null && tar -cf - -C /var/lib/clickhouse/backup/$TMP_BACKUP_NAME ."                                                                                                                               
```