# WAL-G
[![Build Status](https://travis-ci.org/wal-g/wal-g.svg?branch=master)](https://travis-ci.org/wal-g/wal-g)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)

WAL-G is an archival restoration tool for Postgres(beta for MySQL, MongoDB, and Redis)

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4, LZMA, or Brotli compression, multiple processors, and non-exclusive base backups for Postgres. More information on the design and implementation of WAL-G can be found on the Citus Data blog post ["Introducing WAL-G by Citus: Faster Disaster Recovery for Postgres"](https://www.citusdata.com/blog/2017/08/18/introducing-wal-g-faster-restores-for-postgres/).

**Table of Contents**
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Databases](#databases)
- [Development](#development)
	- [Installing](#installing)
	- [Testing](#testing)
	- [Development on windows](#development-on-windows) 
- [Authors](#authors)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [Chat](#chat)

Installation
----------
A precompiled binary for Linux AMD 64 of the latest version of WAL-G can be obtained under the [Releases tab](https://github.com/wal-g/wal-g/releases).

To decompress the binary, use:

```
tar -zxvf wal-g.linux-amd64.tar.gz
mv wal-g /usr/local/bin/
```
For other incompatible systems, please consult the [Development](#development) section for more information.

Configuration
-------------

**One of these variables is required**

To connect to Amazon S3, WAL-G requires that this variable be set:

* `WALG_S3_PREFIX` (e.g. `s3://bucket/path/to/folder`) (alternative form `WALE_S3_PREFIX`)

WAL-G determines AWS credentials [like other AWS tools](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence). You can set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (optionally with `AWS_SESSION_TOKEN`), or `~/.aws/credentials` (optionally with `AWS_PROFILE`), or you can set nothing to fetch credentials from the EC2 metadata service automatically.


To store backups in Google Cloud Storage, WAL-G requires that this variable be set:

* `WALG_GS_PREFIX` to specify where to store backups (e.g. `gs://x4m-test-bucket/walg-folder`) 

WAL-G determines Google Cloud credentials using [application-default credentials](https://cloud.google.com/docs/authentication/production) like other GCP tools. You can set `GOOGLE_APPLICATION_CREDENTIALS` to point to a service account json key from GCP. If you set nothing, WAL-G will attempt to fetch credentials from the GCE/GKE metadata service.


To store backups in Azure Storage, WAL-G requires that this variable be set:

* `WALG_AZ_PREFIX` to specify where to store backups in Azure storage (e.g. `azure://test-container/walg-folder`)

WAL-G determines Azure Storage credentials using [azure default credentials](https://docs.microsoft.com/en-us/azure/storage/common/storage-azure-cli#azure-cli-sample-script). You can set `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_ACCESS_KEY` to provide azure storage credentials.

WAL-G sets default upload buffer size to 64 Megabytes and uses 3 buffers by default. However, users can choose to override these values by setting optional environment variables.


To store backups in Swift object storage, WAL-G requires that this variable be set:

* `WALG_SWIFT_PREFIX` to specify where to store backups in Swift object storage (e.g. `swift://test-container/walg-folder`)

WAL-G determines Swift object storage credentials using [openStack default credentials](https://www.swiftstack.com/docs/cookbooks/swift_usage/auth.html). You can use any of V1, V2, V3 of the SwiftStack Auth middleware to provide Swift object storage credentials.


To store backups on files system, WAL-G requires that these variables be set:

* `WALG_FILE_PREFIX` (e.g. `/tmp/wal-g-test-data`)

Please, keep in mind that by default storing backups on disk along with database is not safe. Do not use it as a disaster recovery plan.

**Optional variables**

* `AWS_REGION`(e.g. `us-west-2`)

WAL-G can automatically determine the S3 bucket's region using `s3:GetBucketLocation`, but if you wish to avoid this API call or forbid it from the applicable IAM policy, specify this variable.

* `AWS_ENDPOINT`

Overrides the default hostname to connect to an S3-compatible service. i.e, `http://s3-like-service:9000`

* `AWS_S3_FORCE_PATH_STYLE`

To enable path-style addressing (i.e., `http://s3.amazonaws.com/BUCKET/KEY`) when connecting to an S3-compatible service that lack of support for sub-domain style bucket URLs (i.e., `http://BUCKET.s3.amazonaws.com/KEY`). Defaults to `false`.

* `GCS_CONTEXT_TIMEOUT`

Default: 1 hour.

* `GCS_NORMALIZE_PREFIX`

Controls the trimming of extra slashes in paths. The default is `true`. To allow restoring from WAL-E archives on GCS, set it to `false` and keep double slashes in `WALG_GS_PREFIX` values.

* `WALG_AZURE_BUFFER_SIZE` (e.g. `33554432`)

Overrides the default `upload buffer size` of 67108864 bytes (64 MB). Note that the size of the buffer must be specified in bytes. Therefore, to use 32 MB sized buffers, this variable should be set to 33554432 bytes.

* `WALG_AZURE_MAX_BUFFERS` (e.g. `5`)

Overrides the default `maximum number of upload buffers`. By default, at most 3 buffers are used concurrently.

* `TOTAL_BG_UPLOADED_LIMIT` (e.g. `1024`)
Overrides the default `number of WAL files to upload during one scan`. By default, at most 32 WAL files will be uploaded.

***Example: Using Minio.io S3-compatible storage***

```
AWS_ACCESS_KEY_ID: "<minio-key>"
AWS_SECRET_ACCESS_KEY: "<minio-secret>"
WALE_S3_PREFIX: "s3://my-minio-bucket/sub-dir"
AWS_ENDPOINT: "http://minio:9000"
AWS_S3_FORCE_PATH_STYLE: "true"
AWS_REGION: us-east-1
WALG_S3_CA_CERT_FILE: "/path/to/custom/ca/file"
```

* `WALG_S3_STORAGE_CLASS`

To configure the S3 storage class used for backup files, use `WALG_S3_STORAGE_CLASS`. By default, WAL-G uses the "STANDARD" storage class. Other supported values include "STANDARD_IA" for Infrequent Access and "REDUCED_REDUNDANCY" for Reduced Redundancy.

* `WALG_S3_SSE`

To enable S3 server-side encryption, set to the algorithm to use when storing the objects in S3 (i.e., `AES256`, `aws:kms`).

* `WALG_S3_SSE_KMS_ID`

If using S3 server-side encryption with `aws:kms`, the KMS Key ID to use for object encryption.

* `WALG_CSE_KMS_ID`

To configure AWS KMS key for client-side encryption and decryption. By default, no encryption is used. (AWS_REGION or WALG_CSE_KMS_REGION required to be set when using AWS KMS key client-side encryption)

* `WALG_CSE_KMS_REGION`

To configure AWS KMS key region for client-side encryption and decryption (i.e., `eu-west-1`).

* `WALG_COMPRESSION_METHOD`

To configure the compression method used for backups. Possible options are: `lz4`, 'lzma', 'brotli'. The default method is `lz4`. LZ4 is the fastest method, but the compression ratio is bad.
LZMA is way much slower. However, it compresses backups about 6 times better than LZ4. Brotli is a good trade-off between speed and compression ratio, which is about 3 times better than LZ4.

**More options are available for the chosen database. See it in [Databases](#databases)**

Usage
-----

WAL-G currently supports these commands for all type of databases:

* ``backup-list``

Lists names and creation time of available backups.

``--pretty``  flag prints list in a table

``--json`` flag prints list in JSON format, pretty-printed if combined with ``--pretty`` 

``--detail`` flag prints extra backup details, pretty-printed if combined with ``--pretty`` , json-encoded if combined with ``--json`` 

* ``delete``

Is used to delete backups and WALs before them. By default ``delete`` will perform a dry run. If you want to execute deletion, you have to add ``--confirm`` flag at the end of the command. Backups marked as permanent will not be deleted.

``delete`` can operate in three modes: ``retain``, ``before`` and ``everything``.

``retain`` [FULL|FIND_FULL] %number% [--after %name|time%]

if FULL is specified keep $number% full backups and everything in the middle. If with --after flag is used keep 
$number$ the most recent backups and backups made after %name|time% (including).

``before`` [FIND_FULL] %name%

If `FIND_FULL` is specified, WAL-G will calculate minimum backup needed to keep all deltas alive. If FIND_FULL is not specified and call can produce orphaned deltas - the call will fail with the list.

``everything`` [FORCE]

Examples: 

``everything`` all backups will be deleted (if there are no permanent backups)

``everything FORCE`` all backups, include permanent, will be deleted

``retain 5`` will fail if 5th is delta

``retain FULL 5`` will keep 5 full backups and all deltas of them

``retain FIND_FULL 5`` will find necessary full for 5th and keep everything after it

``retain 5 --after 2019-12-12T12:12:12`` keep 5 most recent backups and backups made after 2019-12-12 12:12:12

``before base_000010000123123123`` will fail if `base_000010000123123123` is delta

``before FIND_FULL base_000010000123123123`` will keep everything after base of base_000010000123123123

**More commands are available for the chosen database engine. See it in [Databases](#databases)**

Databases
-----------
### PostgreSQL
[Information about installing, configuration and usage](https://github.com/wal-g/wal-g/blob/master/PostgreSQL.md)

### MySQL
[Information about installing, configuration and usage](https://github.com/wal-g/wal-g/blob/master/MySQL.md)

### MariaDB
[Information about installing, configuration and usage](https://github.com/wal-g/wal-g/blob/master/MariaDB.md)

### Mongo
[Information about installing, configuration and usage](https://github.com/wal-g/wal-g/blob/master/MongoDB.md)

Development
-----------
### Installing
It is specified for your type of [database](#databases).

### Testing

WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects. Unit tests can be run using
```
make unittest
```
For more information on testing, please consult [test](test), [testtools](testtools) and `unittest` section in [Makefile](Makefile).

WAL-G will perform a round-trip compression/decompression test that generates a directory for data (e.g. data...), compressed files (e.g. compressed), and extracted files (e.g. extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

Test coverage can be obtained using:
```
make coverage
```
This command generates `coverage.out` file and opens HTML representation of the coverage.
### Development on Windows

[Information about installing and usage](https://github.com/wal-g/wal-g/blob/master/Windows.md)


Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](CONTRIBUTORS) who participated in this project.

License
-------

This project is licensed under the Apache License, Version 2.0, but the lzo support is licensed under GPL 3.0+. Please refer to the [LICENSE.md](LICENSE.md) file for more details.

Acknowledgments
----------------
WAL-G would not have happened without the support of [Citus Data](https://www.citusdata.com/)

WAL-G came into existence as a result of the collaboration between a summer engineering intern at Citus, Katie Li, and Daniel Farina, the original author of WAL-E, who currently serves as a principal engineer on the Citus Cloud team. Citus Data also has an [open source extension to Postgres](https://github.com/citusdata) that distributes database queries horizontally to deliver scale and performance.

WAL-G development is supported by [Yandex Cloud](https://cloud.yandex.com)

Chat
----
We have a [Slack group](https://postgresteam.slack.com/messages/CA25P48P2) and [Telegram chat](https://t.me/joinchat/C03q9FOwa7GgIIW5CwfjrQ) to discuss WAL-G usage and development. To joint PostgreSQL slack use [invite app](https://postgres-slack.herokuapp.com).
