# WAL-G
![Docker-tests-status](https://github.com/wal-g/wal-g/workflows/Docker%20tests/badge.svg)
![Unit-tests-status](https://github.com/wal-g/wal-g/workflows/Unit%20tests/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)

[Full documentation can be found here](https://wal-g.readthedocs.io)

WAL-G is an archival restoration tool for Postgres(beta for MySQL, MariaDB, and MongoDB)

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

```plaintext
tar -zxvf wal-g.linux-amd64.tar.gz
mv wal-g /usr/local/bin/
```
For other incompatible systems, please consult the [Development](#development) section for more information.

Configuration
-------------
### Storage
To configure where WAL-G stores backups, please consult the [Storages](STORAGES.md) section.

### Compression
* `WALG_COMPRESSION_METHOD`

To configure the compression method used for backups. Possible options are: `lz4`, `lzma`, `brotli`. The default method is `lz4`. LZ4 is the fastest method, but the compression ratio is bad.
LZMA is way much slower. However, it compresses backups about 6 times better than LZ4. Brotli is a good trade-off between speed and compression ratio, which is about 3 times better than LZ4.

### Encryption

* `YC_CSE_KMS_KEY_ID`

To configure Yandex Cloud KMS key for client-side encryption and decryption. By default, no encryption is used.

* `YC_SERVICE_ACCOUNT_KEY_FILE`

To configure the name of a file containing private key of Yandex Cloud Service Account. If not set a token from the metadata service (http://169.254.169.254) will be used to make API calls to Yandex Cloud KMS.

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


Usage
-----

**More options are available for the chosen database. See it in [Databases](#databases)**

WAL-G currently supports these commands for all type of databases:

### ``backup-list``

Lists names and creation time of available backups.

``--pretty``  flag prints list in a table

``--json`` flag prints list in JSON format, pretty-printed if combined with ``--pretty``

``--detail`` flag prints extra backup details, pretty-printed if combined with ``--pretty`` , json-encoded if combined with ``--json``

### ``delete``

Is used to delete backups and WALs before them. By default ``delete`` will perform a dry run. If you want to execute deletion, you have to add ``--confirm`` flag at the end of the command. Backups marked as permanent will not be deleted.

``delete`` can operate in four modes: ``retain``, ``before``, ``everything`` and ``target``.

``retain`` [FULL|FIND_FULL] %number% [--after %name|time%]

if FULL is specified keep $number% full backups and everything in the middle. If with --after flag is used keep
$number$ the most recent backups and backups made after %name|time% (including).

``before`` [FIND_FULL] %name%

If `FIND_FULL` is specified, WAL-G will calculate minimum backup needed to keep all deltas alive. If FIND_FULL is not specified and call can produce orphaned deltas - the call will fail with the list.

``everything`` [FORCE]

``target`` [FIND_FULL] %name% | --target-user-data %data% will delete the backup specified by name or user data.

(Only in Postgres) By default, if delta backup is provided as the target, WAL-G will also delete all the dependant delta backups. If `FIND_FULL` is specified, WAL-G will delete all backups with the same base backup as the target.

### Examples

``everything`` all backups will be deleted (if there are no permanent backups)

``everything FORCE`` all backups, include permanent, will be deleted

``retain 5`` will fail if 5th is delta

``retain FULL 5`` will keep 5 full backups and all deltas of them

``retain FIND_FULL 5`` will find necessary full for 5th and keep everything after it

``retain 5 --after 2019-12-12T12:12:12`` keep 5 most recent backups and backups made after 2019-12-12 12:12:12

``before base_000010000123123123`` will fail if `base_000010000123123123` is delta

``before FIND_FULL base_000010000123123123`` will keep everything after base of base_000010000123123123

``target base_0000000100000000000000C9`` delete the base backup and all dependant delta backups

``  target --target-user-data "{ \"x\": [3], \"y\": 4 }"``     delete backup specified by user data

``target base_0000000100000000000000C9_D_0000000100000000000000C4``    delete delta backup and all dependant delta backups

``target FIND_FULL base_0000000100000000000000C9_D_0000000100000000000000C4`` delete delta backup and all delta backups with the same base backup

**More commands are available for the chosen database engine. See it in [Databases](#databases)**

Databases
-----------
### PostgreSQL
[Information about installing, configuration and usage](PostgreSQL.md)

### MySQL/MariaDB
[Information about installing, configuration and usage](MySQL.md)

### SQLServer
[Information about installing, configuration and usage](SQLServer.md)

### Mongo
[Information about installing, configuration and usage](MongoDB.md)

### FoundationDB [Work in progress]
[Information about installing, configuration and usage](FoundationDB.md)



Development
-----------
### Installing
It is specified for your type of [database](#databases).

### Testing

WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects. Unit tests can be run using
```bash
make unittest
```
For more information on testing, please consult [test](test), [testtools](testtools) and `unittest` section in [Makefile](Makefile).

WAL-G will perform a round-trip compression/decompression test that generates a directory for data (e.g. data...), compressed files (e.g. compressed), and extracted files (e.g. extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

Test coverage can be obtained using:
```bash
make coverage
```
This command generates `coverage.out` file and opens HTML representation of the coverage.
### Development on Windows

[Information about installing and usage](Windows.md)


Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](Contributors.md) who participated in this project.

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
We have a [Slack group](https://postgresteam.slack.com/messages/CA25P48P2) and [Telegram chat](https://t.me/joinchat/C03q9FOwa7GgIIW5CwfjrQ) to discuss WAL-G usage and development. To join PostgreSQL slack use [invite app](https://postgres-slack.herokuapp.com).
