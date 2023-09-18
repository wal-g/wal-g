# WAL-G
![Docker-tests-status](https://github.com/wal-g/wal-g/workflows/Docker%20tests/badge.svg)
![Unit-tests-status](https://github.com/wal-g/wal-g/workflows/Unit%20tests/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/wal-g/wal-g)](https://goreportcard.com/report/github.com/wal-g/wal-g)
[![Documentation Status](https://readthedocs.org/projects/wal-g/badge/?version=latest)](https://wal-g.readthedocs.io/?badge=latest)

[This documentation is also available at wal-g.readthedocs.io](https://wal-g.readthedocs.io)

WAL-G is an archival restoration tool for PostgreSQL, MySQL/MariaDB, and MS SQL Server (beta for MongoDB and Redis).

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4, LZMA, ZSTD, or Brotli compression, multiple processors, and non-exclusive base backups for Postgres. More information on the original design and implementation of WAL-G can be found on the Citus Data blog post ["Introducing WAL-G by Citus: Faster Disaster Recovery for Postgres"](https://www.citusdata.com/blog/2017/08/18/introducing-wal-g-faster-restores-for-postgres/).

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
- [Acknowledgments](#acknowledgments)
- [Chat](#chat)

Installation
----------
A precompiled binary for Linux AMD 64 of the latest version of WAL-G can be obtained under the [Releases tab](https://github.com/wal-g/wal-g/releases).

Binary name has the following format: `wal-g-DBNAME-OSNAME`, where `DBNAME` stands for the name of the database (for example pg, mysql), `OSNAME` stands for the name of the operating system used for building the binary.

To decompress the binary, use:

```plaintext
tar -zxvf wal-g-DBNAME-OSNAME-amd64.tar.gz
mv wal-g-DBNAME-OSNAME-amd64 /usr/local/bin/wal-g
```

For example, for Postgres and Ubuntu 18.04:
```plaintext
tar -zxvf wal-g-pg-ubuntu-18.04-amd64.tar.gz
mv wal-g-pg-ubuntu-18.04-amd64 /usr/local/bin/wal-g
```

For other systems, please consult the [Development](#development) section for more information.

WAL-G supports bash and zsh autocompletion. Run `wal-g help completion` for more info.

Configuration
-------------

There are two ways how you can configure WAL-G:

1. Using environment variables

2. Using a config file

   `--config /path` flag can be used to specify the path where the config file is located.

    We support every format that the [viper package](https://github.com/spf13/viper) supports: JSON, YAML, envfile and [others](https://github.com/spf13/viper#reading-config-files).

Every configuration variable mentioned in the following documentation can be specified either as an environment variable or a field in the config file.

### Storage
To configure where WAL-G stores backups, please consult the [Storages](STORAGES.md) section.

### Compression
* `WALG_COMPRESSION_METHOD`

To configure the compression method used for backups. Possible options are: `lz4`, `lzma`, `zstd`, `brotli`. The default method is `lz4`. LZ4 is the fastest method, but the compression ratio is bad.
LZMA is way much slower. However, it compresses backups about 6 times better than LZ4. Brotli and zstd are a good trade-off between speed and compression ratio, which is about 3 times better than LZ4.

### Encryption

* `YC_CSE_KMS_KEY_ID`

To configure Yandex Cloud KMS key for client-side encryption and decryption. By default, no encryption is used.

* `YC_SERVICE_ACCOUNT_KEY_FILE`

To configure the name of a file containing private key of Yandex Cloud Service Account. If not set a token from the metadata service (http://169.254.169.254) will be used to make API calls to Yandex Cloud KMS.

* `WALG_LIBSODIUM_KEY`

To configure encryption and decryption with libsodium. WAL-G uses an [algorithm](https://download.libsodium.org/doc/secret-key_cryptography/secretstream#algorithm) that only requires a secret key. libsodium keys are fixed-size keys of 32 bytes. For optimal cryptographic security, it is recommened to use a random 32 byte key. To generate a random key, you can something like `openssl rand -hex 32` (set `WALG_LIBSODIUM_KEY_TRANSFORM` to `hex`) or `openssl rand -base64 32` (set `WALG_LIBSODIUM_KEY_TRANSFORM` to `base64`).

* `WALG_LIBSODIUM_KEY_PATH`

Similar to `WALG_LIBSODIUM_KEY`, but value is the path to the key on file system. The file content will be trimmed from whitespace characters.

* `WALG_LIBSODIUM_KEY_TRANSFORM`

The transform that will be applied to the `WALG_LIBSODIUM_KEY` to get the required 32 byte key. Supported transformations are `base64`, `hex` or `none` (default).
The option `none` exists for backwards compatbility, the user input will be converted to 32 byte either via truncation or by zero-padding.

* `WALG_GPG_KEY_ID`  (alternative form `WALE_GPG_KEY_ID`) ⚠️ **DEPRECATED**

To configure GPG key for encryption and decryption. By default, no encryption is used. Public keyring is cached in the file "/.walg_key_cache".

* `WALG_PGP_KEY`

To configure encryption and decryption with OpenPGP standard. You can join multiline key using `\n` symbols into one line (mostly used in case of daemontools and envdir).
Set *private key* value when you need to execute ```wal-fetch``` or ```backup-fetch``` command.
Set *public key* value when you need to execute ```wal-push``` or ```backup-push``` command.
Keep in mind that the *private key* also contains the *public key*.

* `WALG_PGP_KEY_PATH`

Similar to `WALG_PGP_KEY`, but value is the path to the key on file system.

* `WALG_PGP_KEY_PASSPHRASE`

If your *private key* is encrypted with a *passphrase*, you should set *passphrase* for decrypt.

* `WALG_ENVELOPE_PGP_KEY`
To configure encryption and decryption with the envelope PGP key stored in key management system.
This option allows you to securely manage your PGP keys by storing them in the KMS.
It is crucial to ensure that the key passed is encrypted using kms and encoded with *base64*.
Also both *private* and *publlic* parts should be presents in key because envelope key will be injected in metadata and used later in `wal/backup-fetch`.

Please note that currently, only Yandex Cloud Key Management Service (KMS) is supported for configuring.
Ensure that you have set up and configured Yandex Cloud KMS mentioned below before attempting to use this feature.

* `WALG_ENVELOPE_CACHE_EXPIRATION`

This setting controls kms response expiration. Default value is `0` to store keys permanent in memory.
Please note that if the system will not be able to redecrypt the key in kms after expiration, the previous response will be used.

* `WALG_ENVELOPE_PGP_KEY_ID`
Envelope Key identifier.
Please note that this key will be used to distinguish one key from another in cache.
If no one identifier was passed sha1 will be calculated over the envelope key.

* `WALG_ENVELOPE_PGP_YC_ENDPOINT`

Endpoint is an API endpoint of Yandex.Cloud against which the SDK is used. Most users won't need to explicitly set it.

* `WALG_ENVELOPE_PGP_YC_CSE_KMS_KEY_ID`

Similar to `YC_CSE_KMS_KEY_ID`, but only used for envelope pgp keys.

* `WALG_ENVELOPE_PGP_YC_SERVICE_ACCOUNT_KEY_FILE`

Similar to `YC_SERVICE_ACCOUNT_KEY_FILE`, but only used for envelope pgp keys.

* `WALG_ENVELOPE_PGP_KEY_PATH`

Similar to `WALG_ENVELOPE_PGP_KEY`, but value is the path to the key on file system.


### Monitoring

* `WALG_STATSD_ADDRESS`

To enable metrics publishing to [statsd](https://github.com/statsd/statsd) or [statsd_exporter](https://github.com/prometheus/statsd_exporter). Metrics will be sent on a best-effort basis via UDP. The default port for statsd is `9125`.

### Profiling

Profiling is useful for identifying bottlenecks within WAL-G.

* `PROFILE_SAMPLING_RATIO`

A float value between 0 and 1, defines likelihood of the profiler getting enabled. When set to 1, it will always run. This allows probabilistic sampling of invocations. Since WAL-G processes may get created several times per second (e.g. wal-g wal-push), we do not want to profile all of them.

* `PROFILE_MODE`

The type of pprof profiler to use. Can be one of `cpu`, `mem`, `mutex`, `block`, `threadcreation`, `trace`, `goroutine`. See the [runtime/pprof docs](https://pkg.go.dev/runtime/pprof) for more information. Defaults to `cpu`.

* `PROFILE_PATH`

The directory to store profiles in. Defaults to `$TMPDIR`.

### Rate limiting
* `WALG_NETWORK_RATE_LIMIT`

Network traffic rate limit during the ```backup-push```/```backup-fetch``` operations in bytes per second.


### Database-specific options
**More options are available for the chosen database. See it in [Databases](#databases)**

Usage
-----

WAL-G currently supports these commands for all type of databases:

### ``backup-list``

Lists names and creation time of available backups.

``--pretty``  flag prints list in a table

``--json`` flag prints list in JSON format, pretty-printed if combined with ``--pretty``

``--detail`` flag prints extra backup details, pretty-printed if combined with ``--pretty``, json-encoded if combined with ``--json``

### ``delete``

Is used to delete backups and WALs before them. By default, ``delete`` will perform a dry run. If you want to execute deletion, you have to add ``--confirm`` flag at the end of the command. Backups marked as permanent will not be deleted.

``delete`` can operate in four modes: ``retain``, ``before``, ``everything`` and ``target``.

``retain`` [FULL|FIND_FULL] %number% [--after %name|time%]

if ``FULL`` is specified, keep ``%number%`` full backups and everything in the middle. If with ``--after`` flag is used keep
$number$ the most recent backups and backups made after ``%name|time%`` (including).

``before`` [FIND_FULL] %name%

If `FIND_FULL` is specified, WAL-G will calculate minimum backup needed to keep all deltas alive. If ``FIND_FULL`` is not specified, and call can produce orphaned deltas, the call will fail with the list.

``everything`` [FORCE]

``target`` [FIND_FULL] %name% | --target-user-data %data% will delete the backup specified by name or user data. Unlike other delete commands, this command does not delete any archived WALs.

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

## Storage tools
`wal-g st` command series allows the direct interaction with the configured storage.
[Storage tools documentation](StorageTools.md)

Databases
-----------
### PostgreSQL
[Information about installing, configuration and usage](PostgreSQL.md)

### MySQL/MariaDB
[Information about installing, configuration and usage](MySQL.md)

### SQLServer
[Information about installing, configuration and usage](SQLServer.md)

### Mongo [Beta]
[Information about installing, configuration and usage](MongoDB.md)

### FoundationDB [Work in progress]
[Information about installing, configuration and usage](FoundationDB.md)

### Redis [Beta]
[Information about installing, configuration and usage](Redis.md)

### Greenplum [Work in progress]
[Information about installing, configuration and usage](Greenplum.md)

Development
-----------

The following steps describe how to build WAL-G for PostgreSQL, but the process is the same for other databases. For example, to build WAL-G for MySQL, use the `make mysql_build` instead of `make pg_build`.

Optional:

- To build with brotli compressor and decompressor, set the `USE_BROTLI` environment variable.
- To build with libsodium, set the `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, set the `USE_LZO` environment variable.

### Installing

#### Ubuntu

```sh
# Install latest Go compiler
sudo add-apt-repository ppa:longsleep/golang-backports 
sudo apt update
sudo apt install golang-go

# Install lib dependencies
sudo apt install libbrotli-dev liblzo2-dev libsodium-dev curl cmake

# Fetch project and build
go get github.com/wal-g/wal-g
cd ~/go/src/github.com/wal-g/wal-g
make deps
make pg_build
main/pg/wal-g --version
```

Users can also install WAL-G by using `make pg_install`. Specifying the `GOBIN` environment variable before installing allows the user to specify the installation location. By default, `make pg_install` puts the compiled binary in the root directory (`/`).

```sh
export USE_BROTLI=1
export USE_LIBSODIUM=1
export USE_LZO=1
make pg_clean
make deps
GOBIN=/usr/local/bin make pg_install
```

#### macOS

```sh
# brew command is Homebrew for Mac OS
brew install cmake
export USE_BROTLI=1
export USE_LIBSODIUM="true" # since we're linking libsodium later
./link_brotli.sh
./link_libsodium.sh
make install_and_build_pg
```

To build on ARM64, set the corresponding `GOOS`/`GOARCH` environment variables:
```
env GOOS=darwin GOARCH=arm64 make install_and_build_pg
```

The compiled binary to run is `main/pg/wal-g`

### Testing

WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects. Unit tests can be run using
```bash
export USE_BROTLI=1
make unittest
```
For more information on testing, please consult [test](test), [testtools](testtools) and `unittest` section in [Makefile](Makefile).

WAL-G will perform a round-trip compression/decompression test that generates a directory for data (e.g., data...), compressed files (e.g., compressed), and extracted files (e.g., extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

Test coverage can be obtained using:
```bash
export USE_BROTLI=1
make coverage
```
This command generates `coverage.out` file and opens HTML representation of the coverage.

### Development on Windows

[Information about installing and usage](Windows.md)


Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](CONTRIBUTORS.md) who participated in this project.

License
-------

This project is licensed under the Apache License, Version 2.0, but the lzo support is licensed under GPL 3.0+. Please refer to the [LICENSE.md](../LICENSE.md) file for more details.

Acknowledgments
----------------
WAL-G would not have happened without the support of [Citus Data](https://www.citusdata.com/)

WAL-G came into existence as a result of the collaboration between a summer engineering intern at Citus, Katie Li, and Daniel Farina, the original author of WAL-E, who currently serves as a principal engineer on the Citus Cloud team. Citus Data also has an [open-source extension to Postgres](https://github.com/citusdata) that distributes database queries horizontally to deliver scale and performance.

WAL-G development is supported by [Yandex Cloud](https://cloud.yandex.com)

Chat
----
We have a [Slack group](https://postgresteam.slack.com/messages/CA25P48P2) and [Telegram chat](https://t.me/joinchat/C03q9FOwa7GgIIW5CwfjrQ) to discuss WAL-G usage and development. To join PostgreSQL slack, use [invite app](https://postgres-slack.herokuapp.com).
