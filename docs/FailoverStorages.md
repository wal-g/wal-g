# Failover archive storages (experimental)
-----------
It's possible to configure WAL-G for using additional "failover" storages, which are used in case the primary storage becomes unavailable.
This might be useful to avoid out-of-disk-space issues.

The following commands support failover storages:
- `wal-push`
- `wal-fetch`
- `wal-prefetch`
- `backup-push`
- `backup-fetch`
- `backup-list`
- `delete` (including all subcommands)

### Configuration
* `WALG_FAILOVER_STORAGES`

A nested section with settings of failover storages with their names. The primary storage settings are taken from the root of the config.
The primary storage is always used if alive. If not, the first alive failover storage is used (for most of the commands, but some commands uses all alive storages). 
The order of failover storages is determined by sorting their names lexicographically.

Please note that to use this feature WAL-G must be configured using a config file, because it's impossible to put this nested structure into an environment variable.

Example:
```bash
WALG_FAILOVER_STORAGES:
    TEST_STORAGE:
        AWS_SECRET_ACCESS_KEY: "S3_STORAGE_KEY_1"
        AWS_ACCESS_KEY_ID: "S3_STORAGE_KEY_ID_1"
        WALE_S3_PREFIX: "s3://some-s3-storage-1/"
    STORAGE2:
        AWS_SECRET_ACCESS_KEY: "S3_STORAGE_KEY_2"
        AWS_ACCESS_KEY_ID: "S3_STORAGE_KEY_ID_2"
        WALE_S3_PREFIX: "s3://some-s3-storage-2/"
    FILE_STORAGE:
        WALG_FILE_PREFIX: "/some/prefix"
```

#### Storage aliveness checking

WAL-G maintains a list of all storage statuses at any given moment, and uses only alive storages during command executions.
This list serves as a cache as we don't want to check every single storage on each I/O operation.

This cache is stored in two locations: in memory and in a file. 
The memory one is shared withing a single WAL-G process, and the file is shared between subsequent WAL-G runs of one or different WAL-G installations.

There are two sources of information that affects storage statuses in the cache:
- Explicit checks. These are separate routines executed on storages to check if they are alive for RO / RW workload.
- Monitoring actual operations. Each time some operation is performed on a storage, its result is reported and applied to the storage's "aliveness" metrika. This metrika is calculated using the [Exponential Moving Average algorithm](https://en.wikipedia.org/wiki/Exponential_smoothing).

Explicit checks are used only if some storage hasn't been used yet at all or wasn't used for a long time so its status TTL has expired.
For example, if a storage got dead and wasn't used by WAL-G commands because of that, its status eventually gets outdated in the cache. In order to return it to the cache the explicit check is performed.

You can find details of configuring the actual operations monitoring in the section below.

* `WALG_FAILOVER_STORAGES_CHECK` (=`true` by default)

Allows to disable aliveness checks at all. In this case, all configured storages are used, and all are considered alive.

* `WALG_FAILOVER_STORAGES_CHECK_TIMEOUT` (=`30s` by default)

Allows to specify a timeout of explicit checks' routines.

* `WALG_FAILOVER_STORAGES_CHECK_SIZE` (=`1mb` by default)

Allows to specify a file size that is uploaded in the RW explicit check.

* `WALG_FAILOVER_STORAGES_CACHE_LIFETIME` (=`15m` by default)

This setting controls the cache TTL for each storage status.

#### Exponential Moving Average configuration
Applying actual operation statuses uses the EMA algorithm. After each operation, the new aliveness metrika is calculated so:

```
new_aliveness = (reported_value * α) + (prev_aliveness * (1 - α))
```

Where `α` is effectively a "sensitivity" of `reported_value` comparing to the `prev_aliveness`.

The `reported_value` is either the operation weight (in case the operation is successful) or 0 (in case it's not).

Different operations have different weights:
- Checking a file for existence: `1000`
- Listing files in a folder: `2000`
- Reading a file: `1000 * log_10(file_size_in_mb)`, but not less than `1000`.
- Writing a file: `1000 * log_10(file_size_in_mb)`, but not less than `1000`.
- Deleting a file: `500`
- Copying a file: `2000`

When aliveness of a dead storage reaches "alive limit", the storage becomes alive.
When aliveness of an alive storage reaches "dead limit", it becomes dead.
Alive limit is greater than dead limit to make it harder to quickly jump between dead and alive states.

In the formula above, `α` is also not a constant, it changes within some limits, depending on the current aliveness value and the storage status (dead / alive).

`α` changes linearly within the configured ranges, and ranges are different for alive and dead storages.
- For an alive storage, `α` is maximal with aliveness = 1.0, and minimal with aliveness = dead limit.
- For a dead storage, `α` is maximal with aliveness = 0, and minimal with aliveness = alive limit.

This can be configured using the following parameters:

* `WALG_FAILOVER_STORAGES_CACHE_EMA_ALIVE_LIMIT` (=`0.99` by default)

Alive limit value.

* `WALG_FAILOVER_STORAGES_CACHE_EMA_DEAD_LIMIT` (=`0.88` by default)

Dead limit value.

* `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MAX` (=`0.05` by default)

Max EMA Alpha value for an alive storage.

* `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MIN` (=`0.01` by default)

Min EMA Alpha value for an alive storage.

* `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MAX` (=`0.5` by default)

Max EMA Alpha value for a dead storage.

* `WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MIN` (=`0.1` by default)

Min EMA Alpha value for a dead storage.

Playground
-----------
If you prefer to use a Docker image, you can directly test WAL-G with this [playground](https://github.com/stephane-klein/playground-postgresql-walg).

Please note, that is a third-party repository, and we are not responsible for it to always work correctly.
