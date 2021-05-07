# WAL-G for FoundationDB

**Work in progress**

You can use wal-g as a tool for encrypting, compressing FoundationDB backups and push/fetch them to/from storage.

Development
-----------
### Installing
To compile and build the binary for FoundationDB:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```plaintext
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make fdb_build
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.
```plaintext
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make fdb_install
```

Usage
-----

### ``backup-fetch``

Command for sending backup from storage to stream in order to restore it in the database.

```bash
wal-g backup-fetch example_backup
```

Variable _WALG_STREAM_RESTORE_COMMAND_ is required for use backup-fetch
(eg. ```TMP_DIR=$(mktemp -d) && chmod 777 $TMP_DIR && tar -xf - -C $TMP_DIR && BACKUP_DIR=$(find $TMP_DIR -mindepth 1 -print -quit) && fdbrestore start -r file://$BACKUP_DIR -w --dest_cluster_file "/etc/foundationdb/fdb.cluster"  1>&2```)

WAL-G can also fetch the latest backup using:

```bash
wal-g backup-fetch LATEST
```

### ``backup-push``

Command for compressing, encrypting and sending backup from stream to storage.

```bash
wal-g backup-push
```

Variable _WALG_STREAM_CREATE_COMMAND_ is required for use backup-push 
(eg. ```TMP_DIR=$(mktemp -d) && chmod 777 $TMP_DIR && fdbbackup start -d file://$TMP_DIR -w 1>&2 && tar -c -C $TMP_DIR .```)


