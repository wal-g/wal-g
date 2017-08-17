# WAL-G

WAL-G 


Installing
----------
A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

Configuration
-------------
**Required**

To connect to Amazon S3, WAL-G requires that these variables be set:

* `WALE_S3_PREFIX` (eg. `s3://bucket/path/to/folder`)
* `AWS_REGION`(eg. `us-west-2`)
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_SECURITY_TOKEN`

Also note that WAL-G uses this environment variable to connect to Postgres:

* `PGHOST`

**Optional**

Concurrency values can be configured using:

* `WALG_MAX_CONCURRENCY`

To configure how many goroutines to use during extraction, use `WALG_MAX_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, use `WALG_UPLOAD_CONCURRENCY`. By default, WAL-G uses 10 streams.



Usage
-----

WAL-G currently supports these commands:


* ``backup-fetch``

When fetching base backups, the user should pass in the name of the backup and a path to a directory to extract to. If this directory does not exist, WAL-G will create it and any dependent subdirectories. 

```
wal-g backup-fetch ~/extract/to/here example-backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch ~/extract/to/here LATEST
```

* ``backup-push``

When uploading backups to S3, the user should pass in the path containing the backup started by Postgres as in:

```
wal-g backup-push /backup/directory/path
```


* ``wal-fetch``

When fetching WAL archives from S3, the user should pass in the archive name and the name of the file to download to. This file should not exist as WAL-G will create it for you.

```
wal-g wal-fetch example-archive new-file-name
```


* ``wal-push``

When uploading WAL archives to S3, the user should pass in the absolute path to where the archive is located.

```
wal-g wal-push /path/to/archive
```

Development
-----------
WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects.

WAL-G will preform a round-trip compression/decompression test that generates a directory for data (eg. data7869...), compressed files (eg. compressed), and extracted files (eg. extracted). These directories will only get cleaned up if the files in the original data directory matches the files in the extracted one.

Test coverage can be obtained using:

```
go test -v -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Testing Tools
WAL-G offers three miniture programs to assist with testing and development:

* [compress](#compress)
* extract
* generate

<a name="compress"/>
**compress**

Text


Authors
-------

* [Daniel Farina](https://github.com/fdr)
* [Katie Li](https://github.com/katie31)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

License
-------

This project is licensed under the Apache License, Version 2.0. 
Please refer to the [LICENSE.md](LICENSE.md) file for more details.
