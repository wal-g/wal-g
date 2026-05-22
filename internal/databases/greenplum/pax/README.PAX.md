# PAX Storage - Internals Reference

Cloudberry documentation on PAX located at `contrib/pax_storage/doc/`[(src)](https://github.com/apache/cloudberry/tree/2.1.0-incubating/contrib/pax_storage/doc).

## 1. Overview

PAX (Partition Attributes Across) is a table access method in `contrib/pax_storage/`[(src)](https://github.com/apache/cloudberry/tree/2.1.0-incubating/contrib/pax_storage).
It combines row-store and column-store properties in the way it described in classic [PAX paper](https://research.cs.wisc.edu/multifacet/papers/vldb01_pax.pdf).

Files are written in the **PORC** format - a Cloudberry-specific derivative of [ORC](https://orc.apache.org/) that reuses only the protobuf schema.
A second variant, **PORC_VEC**, is used by the vectorized executor; layout is the same, only the in-stream datum encoding differs.

Key properties:

- **Immutable data files.** Once a PORC file is closed, it is never modified in place.
- **File-level MVCC** via a heap-backed auxiliary catalog.
- **Row-level deletes** tracked in separate visibility-bitmap files (visimap).
- **Custom TOAST** implementation - does not use `pg_toast.pg_toast_*`.

## 2. File Layout on Disk

All files for a single PAX relation live under one directory:

```
base/<dbOid>/<relfilenode>_pax/N - PORC data file
```

Block files are named by a monotonically increasing integer allocated from `pg_ext_aux.pg_pax_fastsequence`.

For a block numbered `N` the following files may exist:

| File | Present when                | Purpose |
|---|-----------------------------|---|
| `N` | always                      | PORC data file |
| `N.toast` | `ptexistexttoast = true`    | External TOAST |
| `N_<gen-hex>_<xid-hex>.visimap` | `ptvisimapname is not NULL` | Row-level deletion bitmap |

## 3. PORC File Immutability

PORC files are **immutable after close** - not append-only, not mutable.

- INSERT: a new file is created with a new `block_id`.
- DELETE: the PORC file is not touched. A new visimap file is written instead.
- UPDATE: implemented as DELETE + INSERT.
- Compaction / reclustering: the entire file is re-read, filtered through visimap, and written to a new file with a new block_id; the old file is unlinked.

## 4. Auxiliary Catalog

`README.catalog.md`[(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.catalog.md)

PAX creates an auxiliary HEAP-table for each PAX table. It located under the `pg_ext_aux` namespace:

- **`pg_ext_aux.pg_pax_tables`** - used to associate the PAX table (user table) and auxiliary blocks table (e.g. `pg_ext_aux.pg_pax_blocks_17025`).
- **`pg_ext_aux.pg_pax_blocks_<relid>`** - one row per block-file on disk. Columns:
    - `ptblockname` (int) - block id, equal to the file name on disk (0, 1, 2, etc).
    - `pttupcount` (int) - number of tuples in the file.
    - `ptblocksize` (int) - estimated file size.
    - `ptstatistics` (paxauxstats) - serialized protobuf with column stats.
    - `ptvisimapname` (name) - name of the current visimap file, or NULL.
    - `ptexistexttoast` (bool) - whether `<ptblockname>.toast` exists.
    - `ptisclustered` (bool) - whether the file was produced by CLUSTER.
- **`pg_ext_aux.pg_pax_fastsequence`** - next block_id to allocate, per relation, per segment.

Each segment maintains its own set of rows for its local files.

## 5. MVCC

PAX implements MVCC on two levels.

### File-level visibility

The `pg_pax_blocks_<relid>` heap table carries visibility for whole files.
Files inserted by uncommitted transactions are hidden because their aux row's
`xmin` is not visible. Files deleted by the legacy (non-visimap) path have
their aux row removed; once the deleting transaction commits, other
transactions stop seeing them.

### Row-level visibility (visimap)

Row-level deletions are recorded in separate bitmap files. Naming `<block_id>_<generation-hex>_<xid-hex>.visimap`. Each bit corresponds to one tuple offset within the block; 1 = deleted.

Each DELETE operation creates new visimap file and saves it in `ptvisimapname`. The heap MVCC on the aux row update gives atomicity: older snapshots still see the old `ptvisimapname`; newer snapshots see the new one. Visimap files themselves are immutable after close.

### Concurrent DML

Concurrent DELETE/UPDATE against rows in the same block serialize at the aux heap row level (`RowExclusiveLock` during `UpdateVisimap`). Documented in `README.md:32`[(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.md#L32) as "operating at the granularity of individual data files".

## 6. VACUUM

* Regular VACUUM is a no-op for data [(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_method_internal.cc#L294-L299)
* VACUUM FULL / CLUSTER
   * when no clustering index defined - Cloudberry just copies files to the new relfilenode directory.
   * when heap clustering index defined - Cloudberry will apply visimaps, sort data, etc.

### TRUNCATE / DROP

`PaxAuxRelationNontransactionalTruncate` (`pax_aux_table.cc:735`[(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/catalog/pax_aux_table.cc#L735)) removes the entire `_pax` directory.

## 7. TOAST

See `README.toast.md`[(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.toast.md).
PAX implements its own TOAST and does **not** use PostgreSQL's toasts.

### Compress TOAST

Values above `pax.min_size_of_compress_toast` (default 512 KiB) are stored inline in the PORC file as compressed `varattrib_4b`.

### External TOAST

Values above `pax.min_size_of_external_toast` (default 10 MiB) are stored in a sidecar file `<block_id>.toast`. 
Reference to that data is stored in the PORC (PAX) file. Consequence:

- `N.toast` is meaningless without `N` (offsets live in the PORC footer).
- `N` is incomplete without `N.toast` when `ptexistexttoast = true`.

The `.toast` file is written by the writer at the same time as `N` (`orc_writer.cc:295`[(src)](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/orc/orc_writer.cc#L295)).
It is immutable after close.

## 9. WAL Logging

PAX uses a custom resource manager to write WAL records for every file-level
operation. Files are recoverable from WAL alone during archive recovery;
crash recovery skips PAX replay because file writes are fsync'd before
transaction commit. PiTR, however, applies all WAL-records during replay.


# WAL-G Backup / Restore

### What to back up, per segment

1. Start backup.
2. From `pg_ext_aux.pg_pax_tables`, resolve `auxrelid` for each PAX relation.
3. Dump:
    - The relevant row of `pg_ext_aux.pg_pax_tables`.
    - All rows of `pg_ext_aux.pg_pax_blocks_<relid>`.
4. Copy all files from disk (`filepath.Walk`). If file has records in `pg_ext_aux` - route it to the shared `/paxfiles/` directory.
5. Upload all metadata.
6. Stop backup.

### Why the drift does not break correctness

There are several drifts that can be observed:
1. `PaxRelFileStorageMap` built by querying catalog with per-database consistency.
2. `filepath.Walk` can read partially written files.
3. for non-atomic (non S3) storages, wal-g crash can leave partially uploaded files

All this issues covered by following facts/invariants:
1. When last (in chain of incremental backups) extracted, postgres will run WAL-replay to restore point.
2. Cloudberry durably writes PAX files to disk BEFORE writing to `pg_ext_aux.pg_pax_*` tables. And wal-g uploads files to shared `/paxfiles/` folder IFF this file is recorded in `pg_ext_aux.pg_pax_*`. So, partially written files will go to regular tarball upload. No shared directory poisoning.
3. wal-g 'commits' uploading to shared directory `paxfiles` by writing `pax_files_metadata.json`. Then, wal-g can detect that there are some PAX files that not recorded in `pax_files_metadata.json`. Probably, they are broken.


## WAL-G Storage layout

Storage object names in paxfiles are built from `<spc>_<db>_<md5>_<rel>_<filename>_<id>_pax`. Refer to `MakeFileStorageKey` in `pax/storage.go`.

```
segments_005/
    seg0/
        basebackups_005/
            base_000000010000000000000007/
                pax_files_metadata.json
            paxfiles/
                1663_17019_443a6b9778b7f801411069b37724f06d_16384_0_1778071407234433405_pax
                1663_17019_443a6b9778b7f801411069b37724f06d_16384_0_1_35b_visimap_1778071421594374301_pax
```

### Compression & Encryption on upload

PORC files already compressed. So, PAX files are **not re-compressed** by WAL-G when uploaded to `paxfiles/`.

However, they are compressed when they fall through to the regular tar stream.

Encryption (when configured) is applied in both cases.

#### Example for `pax_files_metadata.json`

```json
{
    "Files": {
        "/base/17019/16384_pax/0": {
            "StoragePath": "1663_17019_443a6b9778b7f801411069b37724f06d_16384_0_1778071407234433405_pax",
            "RelNameMd5": "443a6b9778b7f801411069b37724f06d",
            "IsSkipped": true,
            "MTime": "2026-05-06T12:43:25.365360755Z",
            "Kind": "data",
            "FileMode": 384,
            "InitialUploadTS": "2026-05-06T12:43:28.403329843Z"
        },
        "/base/17019/16384_pax/0_1_35b.visimap": {
            "StoragePath": "1663_17019_443a6b9778b7f801411069b37724f06d_16384_0_1_35b_visimap_1778071421594374301_pax",
            "RelNameMd5": "443a6b9778b7f801411069b37724f06d",
            "MTime": "2026-05-06T12:43:38.424645959Z",
            "Kind": "visimap",
            "FileMode": 384,
            "InitialUploadTS": "2026-05-06T12:43:44.304372554Z"
        }
    }
}
```