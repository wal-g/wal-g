# WAL-G
[![Build Status](https://travis-ci.org/wal-g/wal-g.svg?branch=master)](https://travis-ci.org/wal-g/wal-g)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)

WAL-G is an archival restoration tool for Postgres. 

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4 compression, multiple processors and non-exclusive base backups for Postgres. More information on the design and implementation of WAL-G can be found on the Citus Data blog post ["Introducing WAL-G by Citus: Faster Disaster Recovery for Postgres"](https://www.citusdata.com/blog/2017/08/18/introducing-wal-g-faster-restores-for-postgres/).  

**Table of Contents**  
- [Installation](#installation)  
- [Configuration](#configuration)  
- [Usage](#usage)  
- [Development](#development)  
	- [Installing](#installing)  
	- [Testing](#testing)  
- [Authors](#authors)  
- [License](#license)  
- [Acknowledgements](#acknowledgements)

Installation
----------
A precompiled binary for Linux AMD 64 of the latest version of WAL-G can be obtained under the [Releases tab](https://github.com/wal-g/wal-g/releases).

To decompress the binary, use:

```
tar -zxvf wal-g.linux-amd64.tar.gz
```
For other incompatible systems, please consult the Development section for more information.

Configuration
-------------
**Required**

To connect to Amazon S3, WAL-G requires that these variables be set:

* `WALE_S3_PREFIX` (eg. `s3://bucket/path/to/folder`)

WAL-G determines AWS credentials [like other AWS tools](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence). You can set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (optionally with `AWS_SECURITY_TOKEN`), or `~/.aws/credentials` (optionally with `AWS_PROFILE`), or you can set nothing to automatically fetch credentials from the EC2 metadata service.

WAL-G uses [the usual PostgreSQL environment variables](https://www.postgresql.org/docs/current/static/libpq-envars.html) to configure its connection, especially including `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD`/`PGPASSFILE`/`~/.pgpass`.

**Optional**

WAL-G can automatically determine the S3 bucket's region using `s3:GetBucketLocation`, but if you wish to avoid this API call or forbid it from the applicable IAM policy, specify:

* `AWS_REGION`(eg. `us-west-2`)

Concurrency values can be configured using:

* `WALG_DOWNLOAD_CONCURRENCY`

To configure how many goroutines to use during extraction, use `WALG_DOWNLOAD_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, use `WALG_UPLOAD_CONCURRENCY`. By default, WAL-G uses 10 streams.

* `AWS_ENDPOINT`

Overrides the default hostname to connect to an S3-compatible service. i.e, `http://s3-like-service:9000`

* `AWS_S3_FORCE_PATH_STYLE`

To enable path-style addressing(i.e., `http://s3.amazonaws.com/BUCKET/KEY`) when connecting to an S3-compatible service that lack of support for sub-domain style bucket URLs (i.e., `http://BUCKET.s3.amazonaws.com/KEY`). Defaults to `false`.

*** Example: Using Minio.io S3-compatible storage ***

```
AWS_ACCESS_KEY_ID: "<minio-key>"
AWS_SECRET_ACCESS_KEY: "<minio-secret>"
WALE_S3_PREFIX: "s3://my-minio-bucket/sub-dir"
AWS_ENDPOINT: "http://minio:9000"
AWS_S3_FORCE_PATH_STYLE: "true"
AWS_REGION: us-east-1
```

* `WALG_S3_STORAGE_CLASS`

To configure the S3 storage class used for backup files, use `WALG_S3_STORAGE_CLASS`. By default, WAL-G uses the "STANDARD" storage class. Other supported values include "STANDARD_IA" for Infrequent Access and "REDUCED_REDUNDANCY" for Reduced Redundancy.

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
### Installing

To compile and build the binary:

```
go get github.com/wal-g/wal-g
make all
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.

```
export GOBIN=/usr/local/bin
make install
```

### Testing

WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects. For more information on testing, please consult [test_tools](test_tools).

WAL-G will perform a round-trip compression/decompression test that generates a directory for data (eg. data...), compressed files (eg. compressed), and extracted files (eg. extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

Test coverage can be obtained using:

```
go test -v -coverprofile=coverage.out
go tool cover -html=coverage.out
```


Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](CONTRIBUTORS) who participated in this project.

License
-------

This project is licensed under the Apache License, Version 2.0, but the lzo support is licensed under GPL 3.0+. Please refer to the [LICENSE.md](LICENSE.md) file for more details.

Acknowledgements
----------------
WAL-G would not have happened without the support of [Citus Data](https://www.citusdata.com/)

WAL-G came into existence as a result of the collaboration between a summer engineering intern at Citus, Katie Li, and Daniel Farina, the original author of WAL-E who currently serves as a principal engineer on the Citus Cloud team. Citus Data also has an [open source extension to Postgres](https://github.com/citusdata) that distributes database queries horizontally to deliver scale and performance. 