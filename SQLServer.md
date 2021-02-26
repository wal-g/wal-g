WAL-G facilitates SQLServer backups by emulating Azure Blob storage,
allowing You to backup database to URL with standard BACKUP/RESTORE commands.

Backup to URL peculiarities
---------------------------

SQLServer requires URL to look like `https://backup.local/folder/...`

Note:
* domain name required, no plain IP address supported
* default https port (443) required, no custom port supported
* valid HTTPS ceritficate required
* SQLServer credential for URL `https://backup.local/folder/` should exist

So you need to:
* create fake DNS record '127.0.0.5 backup.local' in `C:\Windows\System32\Drivers\etc\hosts`
* create and import self-signed certificate for `backup.local` domain
* create SQLServer login, able to backup/restore any desired database
* create SQLServer credential for URL `https://backup.local/folder/`
    ```
    CREATE CREDENTIAL [https://backup.local/folder]
    WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = 'does_not_matter'
    ```

Configuration
-------------

Your wal-g.yaml config for SQLServer than may look like

    ```
    WALG_FILE_PREFIX: "C:/backup"
    SQLSERVER_BLOB_CERT_FILE: "C:/Path/To/cert.pem"
    SQLSERVER_BLOB_KEY_FILE:  "C:/Path/To/key.pem"
    SQLSERVER_BLOB_LOCK_FILE: "C:/ProgramData/wal-g.lock"
    SQLSERVER_BLOB_HOSTNAME:  "backup.local"
    SQLSERVER_CONNECTION_STRING: "sqlserver://backupuser:backuppass1!@localhost:1433/instance"

    WALG_UPLOAD_CONCURRENCY:   8  # how many block upload requests handle concurrently
    WALG_DOWNLOAD_CONCURRENCY: 8  # how many block read requests handle concurrently 
    ```

Of course, you may use any wal-g storage instead of FILE

You also need some configuration in SQLServer for wal-g to connect it.

    ```
    CREATE LOGIN [backupuser] WITH PASSWORD = 'backuppass1!';
    ALTER SERVER ROLE [sysadmin] ADD MEMBER [backupuser];
    CREATE CREDENTIAL [https://backup.local/basebackups_005]
    WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = 'does_not_matter';
    CREATE CREDENTIAL [https://backup.local/wal_005]
    WITH IDENTITY='SHARED ACCESS SIGNATURE', SECRET = 'does_not_matter';
    ```

S3 Configuration
-------------

SQLServer backups/restores database by 4MB blocks (MAXTRANSFERSIZE).
As we upload every block as a separate file to S3, it makes sense to set S3 part size to the smalles possible value (5MB) to prevent overuse of memory:

    ```
    WALG_S3_MAX_PART_SIZE: 5242880 # 5MB
    ```


Usage
-----

WAL-G SQLServer extension currently supports these commands:

* ``proxy``

```
wal-g proxy
```

Starts Azure Blob emulator on host `SQLSERVER_BLOB_HOSTNAME`, port 443.
Proxy runs in foreground, until Ctrl+C pressed.
While proxy is running, you are able to any BACKUP/RESTORE TO/FROM URL commands in SQLServer.
Proxy intended for debug/manual backups only, it rely on user to maintain proper backups folder structure.
For simple backup/restore process please consider using `backup-push` and `backup-restore` commands.

* ``backup-push``

```
wal-g backup-push
wal-g backup-push -d db1 -d db2
wal-g backup-push -d ALL
```

Backups serveral databases to the backup. 
You can specify which databases to backup via `-d` flag.
You can backup all (including system) databases using `-d ALL` flag.
By default it will backup all non-system databases.

* ``backup-restore``

```
wal-g backup-restore backup_name
wal-g backup-restore LATEST
wal-g backup-restore backup_name -d db1
wal-g backup-restore backup_name -d db1 -n
wal-g backup-restore backup_name -d db1_copy -f db1
```

Restores several databases from backup.
You can specify particular `backup_name` or use `LATEST` alias for the last backup.
You can specify which databases to restore via `-d` flag.
You can restore all (including system) databases using `-d ALL` flag.
You can restore database with new name (create copy of database) using flag `-f` (`--from`)
By default it will restore all non-system databases found in backup.


* ``backup-list``

```
wal-g backup-list
```

* ``delete``

```
wal-g delete retain 3
wal-g delete before backup_name
wal-g delete everything
```
