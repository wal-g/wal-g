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

* `WALG_CLICKHOUSE_BACKUP_PATH`

Root backup folder. Used with `clickhouse-backup`

* `WALG_CLICKHOUSE_CREATE_BACKUP`

Command to create ClickHouse backup, should create backup at WALG_CLICKHOUSE_BACKUP_PATH. Required for backup procedure.


Usage
-----

### ``backup-push``

Creates new backup and sends it to storage. 

Runs `WALG_CLICKHOUSE_CREATE_BACKUP` to create backup.

```bash
wal-g backup-push
```

Typical configurations
-----

### Full backup only - using with `clickhouse-backup`


```bash
 WALG_CLICKHOUSE_BACKUP_PATH="/var/lib/clickhouse/backup"                                                                                                                                   
 WALG_CLICKHOUSE_CREATE_BACKUP="clickhouse-backup create"                                                                                                                               
```