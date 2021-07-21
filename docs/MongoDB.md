# WAL-G for MongoDB

**Interface of MongoDB features is currently unstable**

You can use wal-g as a tool for making encrypted, compressed MongoDB backups and push/fetch them to/from storage without saving it on your filesystem.

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
make mongo_build
```
Users can also install WAL-G by using `make mongo_install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make mongo_install` puts the compiled binary in `go/bin`.
```plaintext
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

* `OPLOG_ARCHIVE_TIMEOUT_INTERVAL`

Time interval (passed since previous upload) to trigger upload to storage.

Format: [golang duration string](https://golang.org/pkg/time/#ParseDuration).

* `MONGODB_LAST_WRITE_UPDATE_INTERVAL`

Interval to update the latest majority optime. wal-g archives only majority committed operations.
Format: [golang duration string](https://golang.org/pkg/time/#ParseDuration).


* `OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY`

Wait for primary and start archiving or exit immediately. 
Archiving works only on primary, but it's useful to run wal-g on all replicaset nodes with `OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY: true` to handle replica set elections. Then new primary will catch up archiving after elections.

* `OPLOG_PITR_DISCOVERY_INTERVAL`

Defines the longest possible point-in-time recovery period.
It's lasts from starting timestamp of the oldest backup (within `OPLOG_PITR_DISCOVERY_INTERVAL`) until now. 
Setting is used by oplog archives [purging](#oplog-purge). 

Format: [golang duration string](https://golang.org/pkg/time/#ParseDuration).


* `OPLOG_PUSH_STATS_ENABLED`

Enables statistics collecting of oplog archiving procedure.

* `OPLOG_PUSH_STATS_UPDATE_INTERVAL`

Interval to update oplog archiving statistics. Disabled if reset to 0.

* `OPLOG_PUSH_STATS_LOGGING_INTERVAL`

Interval to log oplog archiving statistics. Disabled if reset to 0.
Format: [golang duration string](https://golang.org/pkg/time/#ParseDuration).

* `OPLOG_PUSH_STATS_EXPOSE_HTTP`

Exposes http-handler with oplog archiving statistics: `stats/oplog_push`.
HTTP-server listens `HTTP_LISTEN` port (default: 8090).


Usage
-----

WAL-G mongodb extension currently supports these commands:

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

### `backup-show`

Fetches backup metadata from storage to STDOUT.

User should specify the name of the backup to show.

```bash
# wal-g backup-show stream_20201027T224823Z
{
       "BackupName": "stream_20201027T224823Z",
       "DataSize": 18952713762,
       "FinishLocalTime": "2020-10-28T02:08:55.966741+03:00",
       "MongoMeta": {
           "After": {
               "LastMajTS": {
                   "Inc": 34,
                   "TS": 1603840135
               },
               "LastTS": {
                   "Inc": 34,
                   "TS": 1603840135
               }
           },
           "Before": {
               "LastMajTS": {
                   "Inc": 4,
                   "TS": 1603838903
               },
               "LastTS": {
                   "Inc": 4,
                   "TS": 1603838903
               }
           }
       },
       "Permanent": false,
       "StartLocalTime": "2020-10-28T01:48:23.121314+03:00"
}
```
### `backup-delete`

Deletes backup from storage.

User should specify the name of the backup to delete.

Dry-run
```bash
wal-g backup-delete example_backup
```

Perform delete
```bash
wal-g backup-delete example_backup --confirm
```

### `oplog-push`

Fetches oplog from mongodb instance (`MONGODB_URI`) and uploads to storage.

wal-g forces upload when,
  - archive batch exceeds `OPLOG_ARCHIVE_AFTER_SIZE` bytes
  - passes `OPLOG_ARCHIVE_TIMEOUT_INTERVAL` since previous upload

Archiving collects writes if optime is readable by majority reads. Optime is updated every `MONGODB_LAST_WRITE_UPDATE_INTERVAL`.

```bash
wal-g oplog-push
```

Note: archiving works only on primary, but you can run it on any replicaset node using config option `OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY: true`.

### `oplog-replay`

Fetches oplog archives from storage and applies to mongodb instance (`MONGODB_URI`)

User should specify SINCE and UNTIL boundaries (format: `timestamp.inc`, eg `1593554809.32`). Both of them should exist in storage.
SINCE is included and UNTIL is NOT.

```bash
wal-g oplog-replay 1593554109.1 1593559109.1
```

### Common constraints:

- SINCE: operation timestamp before full backup started.
- UNTIL: operation timestamp after backup finished.

Use `MongoMeta.Before.LastMajTS` and `MongoMeta.After.LastMajTS` fields from backup [metadata](#backup-show).

### `oplog-fetch`

Fetches oplog archives from storage and passes to STDOUT.

User should specify SINCE and UNTIL boundaries (format: `timestamp.inc`, eg `1593554809.32`). Both of them should exist in storage.
SINCE is included and UNTIL is NOT.

Supported formats to output: `json`, `bson`, `bson-raw`

```bash
wal-g oplog-fetch 1593554109.1 1593559109.1 --format json
```

### `oplog-purge`

Purges outdated oplog archives from storage. Clean-up will retain:
- oplog archives in [PITR interval](#oplog_pitr_discovery_interval)
- oplog archives within backup creation period

Dry-run
```bash
wal-g oplog-purge
```

Perform clean-up
```bash
wal-g oplog-purge --confirm
```

Typical configurations
-----

### Full backup/restore only

Here's typical wal-g configuration for that case:
```bash
MONGODB_URI:                 'mongodb://user:password@localhost:27018/?authSource=admin&connect=direct&socketTimeoutMS=60000&connectTimeoutMS=10000&tls=true'
WALG_STREAM_CREATE_COMMAND:  'mongodump --archive --oplog --uri="mongodb://user:password@localhost:27018/?authSource=admin&connectTimeoutMS=10000&tls=true"'
WALG_STREAM_RESTORE_COMMAND: 'mongorestore --archive --oplogReplay --uri="mongodb://user:password@localhost:27018/?authSource=admin&connectTimeoutMS=10000&tls=true"'
```

You may also add `--drop` option to restore command. This option drops the collections from the target database before restoring the collections from the dumped backup. Thus extra care should be exercised.

### Continuous archiving and point-in-time recovery 

Here's typical wal-g configuration for that case:
```bash
# Used to fetch oplog documents
MONGODB_URI:                 'mongodb://user:password@localhost:27018/?authSource=admin&connect=direct&socketTimeoutMS=60000&connectTimeoutMS=10000&tls=true'

# Archiving triggers
OPLOG_ARCHIVE_TIMEOUT_INTERVAL: '30s'
OPLOG_ARCHIVE_AFTER_SIZE:       '20971520'

# Oplog-purge setting: retain oplog archives uploaded before the oldest backup created during last 7 days
OPLOG_PITR_DISCOVERY_INTERVAL: '168h'

# Run and wait for become primary (usefull to run on all secondaries)
OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY: 'true'

# Collect statistics on archiving process
OPLOG_PUSH_STATS_ENABLED:           'true'
OPLOG_PUSH_STATS_UPDATE_INTERVAL:   '25s'
# log with interval
OPLOG_PUSH_STATS_LOGGING_INTERVAL:  '30s'
# expose via http "/stats/oplog_push"
OPLOG_PUSH_STATS_EXPOSE_HTTP:       'true'

# Full backup/restore settings
WALG_STREAM_CREATE_COMMAND:  'mongodump --archive --uri="mongodb://user:password@localhost:27018/?authSource=admin&connectTimeoutMS=10000&tls=true"'
WALG_STREAM_RESTORE_COMMAND: 'mongorestore --archive --uri="mongodb://user:password@localhost:27018/?authSource=admin&connectTimeoutMS=10000&tls=true"'
```

### How to restore backup to point in time

Suppose you want to restore your cluster to `2020-10-28T12:11:10+03:00`.
```bash
# wal-g backup-list -v
name                    finish_local_time         ts_before     ts_after      data_size   permanent
stream_20201025T222118Z 2020-10-26T01:50:17+03:00 1603664478.21 1603666217.6  18521875261 false
stream_20201026T220833Z 2020-10-27T01:37:42+03:00 1603750113.39 1603751861.6  18552792676 false
stream_20201027T224823Z 2020-10-28T02:08:55+03:00 1603838903.4  1603840135.34 18952713762 false
```
Pick the closest backup and restore it (don't forget about [constraints](#common-constraints)):
```bash
# wal-g backup-fetch stream_20201027T224823Z
```
Replay oplog from backup `ts_before` to `2020-10-28T12:11:10+03:00`:
```bash
# wal-g oplog-replay 1603838903.4 1603876270.1
```
