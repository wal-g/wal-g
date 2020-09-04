## WAL-G for PostgreSQL

You can use wal-g as a tool for making encrypted, compressed PostgreSQL backups(full and incremental) and push/fetch them to/from storage without saving it on your filesystem.

Development
-----------
### Installing

Prepare on Ubuntu:
```
sudo apt-get install liblzo2-dev
```

To compile and build the binary for Postgres:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make pg_build
```

Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.

```
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make pg_install
```

Configuration
-------------
WAL-G uses [the usual PostgreSQL environment variables](https://www.postgresql.org/docs/current/static/libpq-envars.html) to configure its connection, especially including `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD`/`PGPASSFILE`/`~/.pgpass`.

`PGHOST` can connect over a UNIX socket. This mode is preferred for localhost connections, set `PGHOST=/var/run/postgresql` to use it. WAL-G will connect over TCP if `PGHOST` is an IP address.

* `WALG_DISK_RATE_LIMIT`

To configure disk read rate limit during ```backup-push``` in bytes per second.

* `WALG_NETWORK_RATE_LIMIT`
To configure the network upload rate limit during ```backup-push``` in bytes per second.


Concurrency values can be configured using:

* `WALG_DOWNLOAD_CONCURRENCY`

To configure how many goroutines to use during ```backup-fetch``` and ```wal-fetch```, use `WALG_DOWNLOAD_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, use `WALG_UPLOAD_CONCURRENCY`. By default, WAL-G uses 16 streams.

* `WALG_UPLOAD_DISK_CONCURRENCY`

To configure how many concurrency streams are reading disk during ```backup-push```. By default, WAL-G uses 1 stream.

* `WALG_SENTINEL_USER_DATA`

This setting allows backup automation tools to add extra information to JSON sentinel file during ```backup-push```. This setting can be used e.g. to give user-defined names to backups.

* `WALG_PREVENT_WAL_OVERWRITE`

If this setting is specified, during ```wal-push``` WAL-G will check the existence of WAL before uploading it. If the different file is already archived under the same name, WAL-G will return the non-zero exit code to prevent PostgreSQL from removing WAL.

* `WALG_LIBSODIUM_KEY`

To configure encryption and decryption with libsodium. WAL-G uses an [algorithm](https://download.libsodium.org/doc/secret-key_cryptography/secretstream#algorithm) that only requires a secret key.

* `WALG_LIBSODIUM_KEY_PATH`

Similar to `WALG_LIBSODIUM_KEY`, but value is the path to the key on file system. The file content will be trimmed from whitespace characters.

* `WALG_GPG_KEY_ID`  (alternative form `WALE_GPG_KEY_ID`) ⚠️ **DEPRECATED**

To configure GPG key for encryption and decryption. By default, no encryption is used. Public keyring is cached in the file "/.walg_key_cache".

* `WALG_PGP_KEY`

To configure encryption and decryption with OpenPGP standard. You can join multiline key using `\n` symbols into one line (mostly used in case of daemontools and envdir).
Set *private key* value, when you need to execute ```wal-fetch``` or ```backup-fetch``` command.
Set *public key* value, when you need to execute ```wal-push``` or ```backup-push``` command.
Keep in mind that the *private key* also contains the *public key*.

* `WALG_PGP_KEY_PATH`

Similar to `WALG_PGP_KEY`, but value is the path to the key on file system.

* `WALG_PGP_KEY_PASSPHRASE`

If your *private key* is encrypted with a *passphrase*, you should set *passphrase* for decrypt.

* `WALG_DELTA_MAX_STEPS`

Delta-backup is the difference between previously taken backup and present state. `WALG_DELTA_MAX_STEPS` determines how many delta backups can be between full backups. Defaults to 0.
Restoration process will automatically fetch all necessary deltas and base backup and compose valid restored backup (you still need WALs after start of last backup to restore consistent cluster).
Delta computation is based on ModTime of file system and LSN number of pages in datafiles.

* `WALG_DELTA_ORIGIN`

To configure base for next delta backup (only if `WALG_DELTA_MAX_STEPS` is not exceeded). `WALG_DELTA_ORIGIN` can be LATEST (chaining increments), LATEST_FULL (for bases where volatile part is compact and chaining has no meaning - deltas overwrite each other). Defaults to LATEST.

* `WALG_TAR_SIZE_THRESHOLD`

To configure the size of one backup bundle (in bytes). Smaller size causes granularity and more optimal, faster recovering. It also increases the number of storage requests, so it can costs you much money. Default size is 1 GB (`1 << 30 - 1` bytes).

Usage
-----

* ``backup-fetch``

When fetching base backups, the user should pass in the name of the backup and a path to a directory to extract to. If this directory does not exist, WAL-G will create it and any dependent subdirectories.

```
wal-g backup-fetch ~/extract/to/here example-backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch ~/extract/to/here LATEST
```

#### Reverse delta unpack

Beta feature: WAL-G can unpack delta backups in reverse order to improve fetch efficiency.

[Reverse delta unpack benchmark results](benchmarks/reverse-delta-unpack-26-03-2020.md)
 
To activate this feature, do one of the following:

* set the `WALG_USE_REVERSE_UNPACK`environment variable
* add the --reverse-unpack flag
```
wal-g backup-fetch /path LATEST --reverse-unpack
```

#### Redundant archives skipping

With [reverse delta unpack](#reverse-delta-unpack) turned on, you also can turn on redundant archives skipping.
Since this feature involves both backup creation and restore process, in order to fully enable it you need to do two things:

1. Optional, but recommended. [Enable rating tar ball composer](#rating-composer-mode) for `backup-push`.

2. Enable redundant backup archives skipping during backup-fetch. Do one of the following:
  
* set the `WALG_USE_REVERSE_UNPACK` and `WALG_SKIP_REDUNDANT_TARS` environment variables
* add the `--reverse-unpack` and `--skip-redundant-tars` flags

```  
wal-g backup-fetch /path LATEST --reverse-unpack --skip-redundant-tars
```

* ``backup-push``

When uploading backups to S3, the user should pass in the path containing the backup started by Postgres as in:

```
wal-g backup-push /backup/directory/path
```
If backup is pushed from replication slave, WAL-G will control timeline of the server. In case of promotion to master or timeline switch, backup will be uploaded but not finalized, WAL-G will exit with an error. In this case logs will contain information necessary to finalize the backup. You can use backuped data if you clearly understand entangled risks.

``backup-push`` can also be run with the ``--permanent`` flag, which will mark the backup as permanent and prevent it from being removed when running ``delete``.

#### Rating composer mode

In the rating composer mode, WAL-G places files with similar updates frequencies in the same tarballs during backup creation. This is the recommended option to use in pair with `backup-fetch` [redundant archives skipping](#redundant-archives-skipping). Be aware that although rating composer allows saving more data, it may result in slower backup creation compared to the default tarball composer.

To activate this feature, do one of the following:

* set the `WALG_USE_RATING_COMPOSER`environment variable
* add the --rating-composer flag

```
wal-g backup-push /path --rating-composer
```

* ``wal-fetch``

When fetching WAL archives from S3, the user should pass in the archive name and the name of the file to download to. This file should not exist as WAL-G will create it for you.

WAL-G will also prefetch WAL files ahead of asked WAL file. These files will be cached in `./.wal-g/prefetch` directory. Cache files older than recently asked WAL file will be deleted from the cache, to prevent cache bloat. If the file is requested with `wal-fetch` this will also remove it from cache, but trigger fulfilment of cache with new file.

```
wal-g wal-fetch example-archive new-file-name
```

* ``wal-push``

When uploading WAL archives to S3, the user should pass in the absolute path to where the archive is located.

```
wal-g wal-push /path/to/archive
```

* ``wal-show``

Show information about the WAL storage folder. `wal-show` shows all WAL segment timelines available in storage, displays the available backups for them, and checks them for missing segments.

* if there are no gaps (missing segments) in the range, final status is `OK`
* if there are some missing segments found, final status is `LOST_SEGMENTS`

```
wal-g wal-show
```

By default, `wal-show` shows available backups for each timeline. To turn it off, add the `--without-backups` flag.

By default, `wal-show` output is plaintext table. For detailed JSON output, add the `--detailed-json` flag.

* ``backup-mark``

Backups can be marked as permanent to prevent them from being removed when running ``delete``. Backup permanence can be altered via this command by passing in the name of the backup (retrievable via `wal-g backup-list --pretty --detail --json`), which will mark the named backup and all previous related backups as permanent. The reverse is also possible by providing the `-i` flag.

```
wal-g backup-mark example-backup -i
```


* ``catchup-push``

To create an catchup incremental backup, the user should pass the path to the master Postgres directory and the LSN of the replica
for which the backup is created.

Steps:
1) Stop replica
2) Get replica LSN (for example using pg_controldata command)
3) Start uploading incremental backup on master.

``` bash
wal-g catchup-push /path/to/master/postgres --from-lsn replica_lsn
```


* ``catchup-fetch``

To accept catchup incremental backup created by `catchup-push`, the user should pass the path to the replica Postgres
directory and name of the backup.

``` bash
wal-g catchup-fetch /path/to/replica/postgres backup_name
```
