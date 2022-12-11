# WAL-G for Greenplum

You can use WAL-G as a tool for making encrypted, compressed physical Greenplum backups and push/fetch them to/from the remote storage without saving it on your filesystem.

Configuration
-------------
WAL-G for Greenplum understands the basic configuration options that are [supported by the WAL-G for Postgres](PostgreSQL.md#Configuration), except the advanced features such as delta backups, remote backups, catchup backup, etc.

To configure the backups, the user needs to do two things on each segment host:
1. Create the [configuration file](Greenplum.md#configuration-file)
2. Configure the [WAL archiving](Greenplum.md#wal-archiving)

#### Configuration file
Unlike the WAL-G for Postgres, a config file is a must. It must be placed in the same location on each cluster host, for example, `/etc/wal-g/wal-g.yaml`. Sample configuration file:

```yaml
~ # cat /etc/wal-g/wal-g.yaml
PGDATABASE: "postgres"
WALE_S3_PREFIX: "s3://some/s3/prefix/"
WALG_NETWORK_RATE_LIMIT: 8388608
PGPASSFILE: "/home/gpadmin/.pgpass"
WALG_GP_LOGS_DIR: "/var/log/greenplum"
AWS_ACCESS_KEY_ID: "aws_access_key_id"
WALG_UPLOAD_CONCURRENCY: 5
WALG_PGP_KEY_PATH: "/path/to/PGP_KEY"
WALG_DOWNLOAD_CONCURRENCY: 5
WALE_GPG_KEY_ID: "gpg_key_id"
WALG_DISK_RATE_LIMIT: 167772160
PGUSER: "gpadmin"
GOMAXPROCS: 6
PGHOST: "localhost"
AWS_ENDPOINT: "https://s3-endpoint.host.name"
AWS_SECRET_ACCESS_KEY: "aws_secret_access_key"
WALG_COMPRESSION_METHOD: "brotli"
```

#### WAL archiving
Also, WAL archiving must be configured on each segment primary. Archive command must contain the `--content-id` flag with the content ID of the segment.
```bash
$ vim postgresql.conf
…
wal_level = archive
archive_mode = on	
archive_command = '/usr/bin/wal-g seg wal-push %p --content-id=-1 --config /etc/wal-g/wal-g.yaml'
… 
```

Usage
-----

### ``backup-push``

After the successful configuration, use the `backup-push` command from the coordinator host to create a new backup.  

```bash
wal-g backup-push --config=/path/to/config.yaml
```

#### Delta backups (work in progress)

* `WALG_DELTA_MAX_STEPS`

Delta-backup is the difference between previously taken backup and present state. `WALG_DELTA_MAX_STEPS` determines how many delta backups can be between full backups. Defaults to 0.
Restoration process will automatically fetch all necessary deltas and base backup and compose valid restored backup (you still need WALs after start of last backup to restore consistent cluster).

Delta computation is based on ModTime of file system and LSN number of pages in datafiles for heap relations and on ModCount + EOF combination for AO/AOCS relations.

##### Create delta from specific backup
When creating delta backup (`WALG_DELTA_MAX_STEPS` > 0), WAL-G uses the latest backup as the base by default. This behaviour can be changed via following flags:

* `--delta-from-name` flag or `WALG_DELTA_FROM_NAME` environment variable to choose the backup with specified name as the base for the delta backup

* `--delta-from-user-data` flag or `WALG_DELTA_FROM_USER_DATA` environment variable to choose the backup with specified user data as the base for the delta backup

Examples:
```bash
wal-g backup-push --delta-from-name backup_name --config=/path/to/config.yaml
wal-g backup-push --delta-from-user-data "{ \"x\": [3], \"y\": 4 }" --config=/path/to/config.yaml
```

### ``backup-fetch``

When fetching base backups, the user should pass in the cluster restore configuration and the name of the backup.
```bash
wal-g backup-fetch backup_20211202T011501Z --restore-config=/path/to/restore_cfg.json --config=/path/to/config.yaml
```

WAL-G can also fetch the latest backup:

```bash
wal-g backup-fetch LATEST --restore-config=/path/to/restore_cfg.json --config=/path/to/config.yaml
```

Cluster restore configuration declares destination host, directory, and port for each segment.  Sample restore configuration:
```json
{
        "segments": {
                "-1": {
                        "hostname": "gp6master",
                        "port": 5432,
                        "data_dir": "/gpdata/master/gpseg-1"
                },
                "0": {
                        "hostname": "gp6segment1",
                        "port": 6000,
                        "data_dir": "/gpdata/primary/gpseg0"
                },
                "1": {
                        "hostname": "gp6segment1",
                        "port": 6001,
                        "data_dir": "/gpdata/primary/gpseg1"
                }
        }
}
```

WAL-G can fetch the backup with specific UserData (stored in backup metadata) using the `--target-user-data` flag or `WALG_FETCH_TARGET_USER_DATA` variable:
```bash
wal-g backup-fetch --target-user-data "{ \"x\": [3], \"y\": 4 }" --restore-config=/path/to/restore_config.json --config=/path/to/config.yaml
```

#### Partial restore
`--content-ids` flag allows to perform the fetch operations only on some specific segments. This might be useful when the backup-fetch operation is completed successfully on all segments except the few ones so the DBA or script can semi-automatically complete the failed backup fetch. For example:
```bash
wal-g backup-fetch LATEST --content-ids=3,5,7 --restore-config=restore-config.json --config=/etc/wal-g/wal-g.yaml
```

#### Backup fetch mode
`--mode` allows to specify the desired mode of the backup-fetching.

- `default` will do the backup unpacking and prepare the configs [unpack+prepare]
- `unpack` will do backup unpacking only
- `prepare` will perform config preparation only.

```bash
wal-g backup-fetch LATEST --mode=unpack --restore-config=restore-config.json --config=/etc/wal-g/wal-g.yaml
```

#### In-place restore
WAL-G can also do in-place backup restoration without the restore config. It might be useful when restoring to the same hosts that were used to make a backup:
```bash
wal-g backup-fetch LATEST --in-place --config=/path/to/config.yaml
```

#### Delete concurrency
During the delete execution, WAL-G can process segments in parallel mode. To control, how many segments will be processed simultaneously, use the `WALG_GP_DELETE_CONCURRENCY` setting. The default value is `1`. 


#### AO/AOCS size threshold
To control the minimal size of the AO/AOCS segment file to be uploaded into the shared storage, use the `WALG_GP_AOSEG_SIZE_THRESHOLD`. The higher this value, the bigger the size of a single backup and the smaller the size of the shared AO/AOCS storage folder. Default value is `1048576 (1MB)`.

### ``restore-point-list``

Lists currently available restore points in storage.

Usage:
```bash
wal-g restore-point-list [--pretty] [--json]
```