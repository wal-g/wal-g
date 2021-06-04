# WAL-G for ClickHouse

**Work in progress**

You can use wal-g as a tool for making encrypted, compressed ClickHouse backups and push them to storage.

Development
-----------
### Installing
To compile and build the binary:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```plaintext
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make clickhouse_build
```
Users can also install WAL-G by using `make clickhouse_install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make clickhouse_install` puts the compiled binary in `go/bin`.
```plaintext
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make clickhouse_install
```

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