## WAL-G for MariaDB

**Interface of MariaDB now is unstable**

You can use wal-g as a tool for encrypting, compressing MariaDB backups and push/fetch them to/from storage without saving it on your filesystem.

Development
-----------
### Installing
To compile and build the binary for MariaDB you have to do the same as for MySQL:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mysql_build
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.
```
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mysql_install
```

Configuration
-------------

* `WALG_MYSQL_DATASOURCE_NAME`

To configure the connection string for MariaDB. Required. Format ```user:password@tcp(host)/dbname```

* `WALG_MYSQL_SSL_CA`

To use SSL, a path to file with certificates should be set to this variable.

* `WALG_STREAM_CREATE_COMMAND`

Command to create MariaDB backup, should return backup as single stream to STDOUT. Requried.

* `WALG_STREAM_RESTORE_COMMAND`

Command to unpack MariaDB backup, should take backup (created by `WALG_STREAM_CREATE_COMMAND`) 
to STDIN and unpack it to MariaDB datadir. Required.

* `WALG_MYSQL_BACKUP_PREPARE_COMMAND`

Command to prepare MariaDB backup after restoring. Optional. Needed for xtrabackup case.

* `WALG_MYSQL_BINLOG_REPLAY_COMMAND`

Command to replay binlog on runing MariaDB. Required for binlog-fetch command.

* `WALG_MYSQL_BINLOG_DST`

To place binlogs in the specified directory during binlog-fetch or binlog-replay     

> **Important**: To make wal-g work with mariadb, you need to [activate the binary log](https://mariadb.com/kb/en/activating-the-binary-log/) by starting mariadb with [--log-bin](https://mariadb.com/kb/en/replication-and-binary-log-server-system-variables/#log_bin) and [--log-basename](https://mariadb.com/kb/en/mysqld-options/#-log-basename)=\[name\].


Usage
-----

WAL-G mysql extension currently supports these commands:

* ``backup-push``

Creates new backup and send it to storage. Runs `WALG_STREAM_CREATE_COMMAND` to create backup.

```
wal-g backup-push
```

* ``backup-list``

Lists currently available backups in storage

```
wal-g backup-list
```

* ``backup-fetch``

Fetches backup from storage and restores it to datadir.
Runs `WALG_STREAM_RESTORE_COMMAND` to restore backup.
User should specify the name of the backup to fetch.

```
wal-g backup-fetch example_backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch  LATEST
```

* ``binlog-push``

Sends (not yet archived) binlogs to storage. Typically run in CRON.

```
wal-g binlog-push
```

* ``binlog-fetch``

Fetches binlogs from storage and saves them to `WALG_MYSQL_BINLOG_DST` folder.
User should specify the name of the backup starting with which to fetch an binlog.
User may also specify time in  RFC3339 format until which should be fetched (used for PITR).
User have to replay binlogs manually in that case.

```
wal-g binlog-fetch --since "backupname"
```
or
```
wal-g binlog-fetch --since "backupname" --until "2006-01-02T15:04:05Z07:00"
```
or
```
wal-g binlog-fetch --since LATEST --until "2006-01-02T15:04:05Z07:00"
```

* ``binlog-replay``

Fetches binlogs from storage and passes them to `WALG_MYSQL_BINLOG_REPLAY_COMMAND` to replay on running MariaDB server.
User should specify the name of the backup starting with which to fetch an binlog.
User may also specify time in  RFC3339 format until which should be fetched (used for PITR).
Binlogs are temporarily save in `WALG_MYSQL_BINLOG_DST` folder.
Replay command gets name of binlog to replay via environment variable `WALG_MYSQL_CURRENT_BINLOG` and stop-date via `WALG_MYSQL_BINLOG_END_TS`, which are set for each invocation.

```
wal-g binlog-replay --since "backupname"
```
or
```
wal-g binlog-replay --since "backupname" --until "2006-01-02T15:04:05Z07:00"
```
or
```
wal-g binlog-replay --since LATEST --until "2006-01-02T15:04:05Z07:00"
```


Typical configuration for using with mariabackup
-----

It's recommended to use wal-g with mariabackup tool for creating lock-less backups.
Here's typical wal-g configuration for that case:
```
 WALG_MYSQL_DATASOURCE_NAME=user:pass@tcp(localhost:3305)/mysql                                                                                                                                      
 WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=xbstream --datadir=/var/lib/mysql"                                                                                                                               
 WALG_STREAM_RESTORE_COMMAND="xbstream -x -C /var/backups/mariadb"                                                                                                                       
 WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=/var/backups/mariadb"                                                                                              
 WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
```

Restore procedure is a bit tricky, but you can follow the major part of [the offical docs for full backup and restore](https://mariadb.com/kb/en/full-backup-and-restore-with-mariabackup/):
* stop mariadb
* clean a datadir (typically `/var/lib/mysql`)
* fetch and prepare desired backup using `wal-g backup-fetch "backup_name"`
* restore with either [`--copy-back`](https://mariadb.com/kb/en/mariabackup-options/#-copy-back) or [`--move-back`](https://mariadb.com/kb/en/mariabackup-options/#-move-back) argument of `mariabackup`:
```
mariabackup --move-back --target-dir=/var/backups/mysql
```
* after the above command you might have to fix file permissions: `chown -R mysql:mysql /var/lib/mysql`
* for PITR, replay binlogs with
```
wal-g binlog-replay --since "backup_name" --until  "2006-01-02T15:04:05Z07:00"
```
* start mariadb


Typical configuration for using with mysqldump
-----

It's possible to use wal-g with standard mysqldump/mysql tools.
In that case MariaDB the backup is a plain SQL script.
Here's typical wal-g configuration for that case:

```
 WALG_MYSQL_DATASOURCE_NAME=user:pass@localhost/mysql                                                                                                               
 WALG_STREAM_CREATE_COMMAND="mysqldump --all-databases --single-transaction --set-gtid-purged=ON"                                                                                                                               
 WALG_STREAM_RESTORE_COMMAND="mysql"
 WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
```

Restore procedure is straightforward:
* start mariadb (it's recommended to create new mariadb instance)
* fetch and apply desired backup using `wal-g backup-fetch "backup_name"`
