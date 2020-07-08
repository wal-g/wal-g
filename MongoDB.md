## WAL-G for MongoDB

**Interface of Mongo now is unstable**

You can use wal-g as a tool for encrypting, compressing Mongo backups and push/fetch them to/from storage without saving it on your filesystem.

Development
-----------
### Installing
To compile and build the binary for Mongo:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mongo_build
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.
```
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mongo_install
```

Configuration
-------------

* `WALG_STREAM_CREATE_COMMAND`

Command to create MongoDB backup, should return backup as single stream to STDOUT. Required for backup procedure.

* `WALG_STREAM_RESTORE_COMMAND`

Command to unpack MongoDB backup, should take backup (created by `WALG_STREAM_CREATE_COMMAND`) 
to STDIN and push it to MongoDB instance. Required for restore procedure.

* `MONGODB_URI`

URI used to connect to a MongoDB instance. Required for backup and oplog archiving procedure.

* `OPLOG_ARCHIVE_AFTER_SIZE`

Oplog archive batch in bytes which triggers upload to storage.

* `OPLOG_ARCHIVE_TIMEOUT`

Timeout in seconds (passed since previous upload) to trigger upload to storage.

* `OPLOG_PUSH_STATS_ENABLED`

Enables statistics collecting of oplog archiving procedure.

* `OPLOG_PUSH_STATS_UPDATE_INTERVAL`

Interval in seconds to update oplog archiving statistics. Disabled if reset to 0.

* `OPLOG_PUSH_STATS_LOGGING_INTERVAL`

Interval in seconds to log oplog archiving statistics. Disabled if reset to 0.

* `OPLOG_PUSH_STATS_EXPOSE_HTTP`

Exposes http-handler with oplog archiving statistics.

* `MONGODB_LAST_WRITE_UPDATE_SECONDS`

Interval in seconds to update the latest majority optime.

Usage
-----

WAL-G mongodb extension currently supports these commands:

* ``backup-push``

Creates new backup and send it to storage.

Runs `WALG_STREAM_CREATE_COMMAND` to create backup.

```
wal-g backup-push
```

* ``backup-list``

Lists currently available backups in storage

```
wal-g backup-list
```

* ``backup-fetch``

Fetches backup from storage and restores passes data to `WALG_STREAM_RESTORE_COMMAND` to restore backup.

User should specify the name of the backup to fetch.

```
wal-g backup-fetch example_backup
```

* ``backup-show``

Fetches backup metadata from storage to STDOUT.

User should specify the name of the backup to show.

```
wal-g backup-show example_backup
```


* ``oplog-push``

Fetches oplog from mongodb instance (`MONGODB_URI`) and uploads to storage.

Upload is forced when,
  - archive batch exceeds `OPLOG_ARCHIVE_AFTER_SIZE` bytes
  - passes `OPLOG_ARCHIVE_TIMEOUT` seconds since previous upload

Archiving collects writes if optime is readable by majority reads. Optime is updated every `MONGODB_LAST_WRITE_UPDATE_SECONDS` seconds.  

```
wal-g oplog-push
```

* ``oplog-replay``

Fetches oplog archives from storage and applies to mongodb instance (`MONGODB_URI`)

User should specify SINCE and UNTIL boundaries (format: `timestamp.inc`, eg `1593554809.32`). Both of them should exist in storage.
SINCE is included and UNTIL is NOT.

```
wal-g oplog-replay 1593554109.1 1593559109.1
```

* ``oplog-fetch``

Fetches oplog archives from storage and passes to STDOUT.

User should specify SINCE and UNTIL boundaries (format: `timestamp.inc`, eg `1593554809.32`). Both of them should exist in storage.
SINCE is included and UNTIL is NOT.

Supported formats to output: `json`, `bson`, `bson-raw`

```
wal-g oplog-fetch 1593554109.1 1593559109.1 --format json
```
