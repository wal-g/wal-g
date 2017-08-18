# WAL-G
[![Build Status](https://travis-ci.org/wal-g/wal-g.svg?branch=master)](https://travis-ci.org/wal-g/wal-g)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)

WAL-G is an archival restoration tool for Postgres. 

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4 compression, multiple processors and non-exclusive base backups for Postgres. More information on the design and implementation of WAL-G can be found on the Citus Data blog post ["BLOG NAME"](https://www.citusdata.com/blog/).  

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
* `AWS_REGION`(eg. `us-west-2`)
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

Also note that WAL-G uses this environment variable to connect to Postgres:

* `PGHOST`

**Optional**

Required if using AWS STS:

* `AWS_SECURITY_TOKEN`

Concurrency values can be configured using:

* `WALG_DOWNLOAD_CONCURRENCY`

To configure how many goroutines to use during extraction, use `WALG_DOWNLOAD_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

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

WAL-G will preform a round-trip compression/decompression test that generates a directory for data (eg. data...), compressed files (eg. compressed), and extracted files (eg. extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

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
WAL-G could not have been possible without the support of [Citus](https://www.citusdata.com/) [Data](https://github.com/citusdata). We would like to express our sincere gratitude and appreciation for having the opportunity to develop and test this project. Thank you to all who contributed to the creation of WAL-G.