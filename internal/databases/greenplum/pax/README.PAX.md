# PAX Storage — Internals Reference

This document summarizes how PAX storage works in Cloudberry, with focus on
aspects that matter for operators, tool authors, and anyone building
backup/restore, replication, or migration tooling.

Authoritative upstream docs live in
[`contrib/pax_storage/doc/`](https://github.com/apache/cloudberry/tree/2.1.0-incubating/contrib/pax_storage/doc).
This file cross-links into the source tree for verification. All links point
to the `2.1.0-incubating` tag.

## 1. Overview

PAX (Partition Attributes Across) is a table access method in
[`contrib/pax_storage/`](https://github.com/apache/cloudberry/tree/2.1.0-incubating/contrib/pax_storage).
It combines row-store (NSM) and column-store (DSM) properties: data is grouped
into files called "micro-partitions", each file stores columns separately
internally, but the unit of transactional visibility is the file.

Files are written in the **PORC** format — a Cloudberry-specific derivative of
ORC that reuses only the protobuf schema. A second variant, **PORC_VEC**, is
used by the vectorized executor; layout is the same, only the in-stream datum
encoding differs.

Key properties:

- **Immutable data files.** Once a PORC file is closed, it is never modified
  in place.
- **File-level MVCC** via a heap-backed auxiliary catalog.
- **Row-level deletes** tracked in separate visibility-bitmap files (visimap).
- **Custom TOAST** implementation — does not use `pg_toast.pg_toast_*`.

## 2. File Layout on Disk

All files for a single PAX relation live under one directory:

```
base/<dbOid>/<relfilenode>_pax/
```

The `_pax` suffix is appended by `paxc::BuildPaxDirectoryPath`
([`paxc_wrappers.cc:102`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/comm/paxc_wrappers.cc#L102)).
Block files are named by a monotonically increasing integer allocated from
`pg_ext_aux.pg_pax_fastsequence`.

For a block numbered `N` the following files may exist:

| File | Present when | Purpose |
|---|---|---|
| `N` | always | PORC data file |
| `N.toast` | `ptexistexttoast = true` | External TOAST sidecar |
| `N_<gen-hex>_<xid-hex>.visimap` | block has had at least one DELETE | Row-level deletion bitmap |

Writer open flags
([`file_system.h:42-45`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/file_system.h#L42-L45)):

```cpp
const int kWriteMode          = O_CREAT | O_WRONLY | O_EXCL;
const int kWriteWithTruncMode = O_CREAT | O_WRONLY | O_TRUNC;
const int kReadWriteMode      = O_CREAT | O_RDWR   | O_EXCL;
```

There is no `O_APPEND` path. Files are either freshly created, or truncated
and rewritten (the truncate mode exists only to handle leftover files from
aborted transactions; see
[`pax.cc:213-224`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L213-L224)).

## 3. PORC File Immutability

PORC files are **immutable after close** — not append-only, not mutable.

- INSERT: a new file is created with a new `block_id`.
- DELETE: the PORC file is not touched. A new visimap file is written instead
  (see §5).
- UPDATE: implemented as DELETE + INSERT (split update, see
  [`README_CTID_in_pax.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/README_CTID_in_pax.md)).
- Compaction / reclustering: the entire file is re-read, filtered through
  visimap, and written to a new file with a new block_id; the old file is
  unlinked.

This is the same model as Apache Iceberg / Parquet / ORC: data files are
write-once, and mutations produce new files plus metadata updates.

## 4. Auxiliary Catalog

PAX stores per-file metadata in heap-backed catalog tables under the
`pg_ext_aux` namespace (see
[`README.catalog.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.catalog.md)):

- **`pg_ext_aux.pg_pax_tables`** — maps PAX relation OID to auxiliary blocks
  relation OID.
- **`pg_ext_aux.pg_pax_blocks_<relid>`** — one row per file. Columns:
    - `ptblockname` (int) — block id, equal to the file name on disk.
    - `pttupcount` (int) — number of tuples in the file.
    - `ptblocksize` (int) — estimated file size.
    - `ptstatistics` (paxauxstats) — serialized protobuf with column stats.
    - `ptvisimapname` (name) — name of the current visimap file, or NULL.
    - `ptexistexttoast` (bool) — whether `<ptblockname>.toast` exists.
    - `ptisclustered` (bool) — whether the file was produced by CLUSTER.
- **`pg_ext_aux.pg_pax_fastsequence`** — next block_id to allocate, per
  relation, per segment.

Each segment maintains its own set of rows for its local files.

## 5. MVCC

PAX implements MVCC on two levels.

### File-level visibility

The `pg_pax_blocks_<relid>` heap table carries visibility for whole files.
A PAX scan iterates aux rows with the caller's snapshot
([`micro_partition_iterator.cc:122`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/micro_partition_iterator.cc#L122)):

```cpp
desc_ = systable_beginscan(aux_rel_, InvalidOid, false, snapshot_, 0, NULL);
```

Files inserted by uncommitted transactions are hidden because their aux row's
`xmin` is not visible. Files deleted by the legacy (non-visimap) path have
their aux row removed; once the deleting transaction commits, other
transactions stop seeing them.

### Row-level visibility (visimap)

Row-level deletions are recorded in separate bitmap files. Naming
([`pax.cc:651-654`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L651-L654)):

```
<block_id>_<generation-hex>_<xid-hex>.visimap
```

Each bit corresponds to one tuple offset within the block; 1 = deleted. Each
DELETE operation on a block:

1. Reads the current visimap (if `ptvisimapname` is not NULL) under the
   caller's snapshot.
2. Unions it with the newly-deleted tuple bitmap.
3. Writes a **new** visimap file with `generation + 1`.
4. Updates the aux row's `ptvisimapname` to point to the new file.

The heap MVCC on the aux row update gives atomicity: older snapshots still see
the old `ptvisimapname`; newer snapshots see the new one. Visimap files
themselves are immutable after close.

At read time, the reader loads the visimap named by the aux row
([`pax.cc:454-462`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L454-L462))
and passes it to the file reader, which filters out deleted tuples.

### Concurrent DML

Concurrent DELETE/UPDATE against rows in the same block serialize at the aux
heap row level (`RowExclusiveLock` during `UpdateVisimap`). Documented in
[`README.md:32`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.md#L32)
as "operating at the granularity of individual data files".

## 6. VACUUM

### Regular VACUUM is a no-op for data

[`pax_access_method_internal.cc:86-91`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_method_internal.cc#L86-L91):

```cpp
void PaxAccessMethod::RelationVacuum(Relation, VacuumParams *,
                                     BufferAccessStrategy) {
  /* PAX: micro-partitions have no dead tuples, so vacuum is empty */
}
```

Because PORC files are immutable, there are no in-file dead tuples to reclaim.
The standard heap vacuum path still runs on the auxiliary tables
(`pg_pax_blocks_<relid>`, `pg_pax_fastsequence`) to clean up deleted aux rows
and freeze xids — that is handled by the ordinary heap AM.

Old visimap-file generations and data files orphaned by aborted transactions
are **not** cleaned up by `VACUUM`. There is a TODO for this in
[`pax_table_cluster.cc:58`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_table_cluster.cc#L58).

### VACUUM FULL / CLUSTER rewrites files

Both go through the `relation_copy_for_cluster` callback
([`pax_access_method_internal.cc:318-345`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_method_internal.cc#L318-L345)),
which calls either:

- `PaxAuxRelationCopyDataForCluster` — copy files to a new relfilenode
  unchanged.
- `IndexCluster` — read tuples through the visimap filter, sort by a btree
  index, and write fresh PORC files.

After `VACUUM FULL` / `CLUSTER` the new relfilenode has no visimap files, no
orphan files, and external TOAST is consolidated.

### TRUNCATE / DROP

`PaxAuxRelationNontransactionalTruncate`
([`pax_aux_table.cc:735`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/catalog/pax_aux_table.cc#L735))
removes the entire `_pax` directory.

## 7. TOAST

See
[`README.toast.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.toast.md).
PAX implements its own TOAST and does **not** use
`pg_toast.pg_toast_<reltoastrelid>`. `RelationNeedsToastTable` returns `false`
([`pax_access_handle.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_handle.cc)).

### Compress TOAST

Values above `pax.min_size_of_compress_toast` (default 512 KiB) that fit
under 1 GiB are stored inline in the PORC file as compressed `varattrib_4b`
— the same layout heap uses. No separate file. Transparent to backup.

### External TOAST

Values above `pax.min_size_of_external_toast` (default 10 MiB) are stored in
a sidecar file `<block_id>.toast`. In the PORC file the column carries a
custom-tagged varlena pointer:

```cpp
struct pax_varatt_external {
    int32  va_rawsize;
    uint32 va_extinfo;
    uint64 va_extogsz;
    uint64 va_extoffs;   // offset inside .toast
    uint64 va_extsize;   // length inside .toast
};
```

Offsets into `.toast` are recorded in the PORC stripe metadata
(`StripeInformation.toastOffset`, `toastLength`, `extToastLength`). Consequence:

- `N.toast` is meaningless without `N` (offsets live in the PORC footer).
- `N` is incomplete without `N.toast` when `ptexistexttoast = true`.

The `.toast` file is written by the writer at the same time as `N`
([`orc_writer.cc:295`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/orc/orc_writer.cc#L295))
with the same open-flag semantics; it is immutable after close.

## 8. Backup / Restore

PORC, visimap, and `.toast` files are all immutable after close, so copying
them under load is safe **provided** the reference set is captured under a
consistent MVCC snapshot of the auxiliary catalog.

### What to back up, per segment, per relation

1. Open a `REPEATABLE READ` (or `SERIALIZABLE`) transaction to get a stable
   snapshot.
2. From `pg_ext_aux.pg_pax_tables`, resolve `auxrelid` for each PAX relation.
3. Dump, under the same snapshot:
    - The relevant row of `pg_ext_aux.pg_pax_tables`.
    - All rows of `pg_ext_aux.pg_pax_blocks_<relid>`.
    - The relevant row of `pg_ext_aux.pg_pax_fastsequence`.
4. For each row in `pg_pax_blocks_<relid>`, copy from
   `base/<dbOid>/<relfilenode>_pax/`:
    - `<ptblockname>` (always).
    - `<ptblockname>.toast` if `ptexistexttoast = true`.
    - `<ptvisimapname>` if not NULL.

**Do not** archive the directory verbatim. It can contain:

- Orphaned visimap files from earlier generations (no aux row references them).
- Data/toast files from aborted transactions (fastsequence advanced but no
  aux row was committed — see
  [`pax.cc:213-224`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L213-L224)).

Iterating by aux-row naturally excludes these.

### Distribution

Each Cloudberry segment owns its own set of files and its own
`pg_pax_blocks_<relid>` / `pg_pax_fastsequence` rows. Back up per segment;
use a distributed snapshot (e.g. `gp_dist_random(...)`) to anchor consistency
across segments.

### Restore

1. Create the PAX table on the target with matching DDL and reloptions.
2. The new relation has different `relfilenode` and aux-relation OID. Place
   the restored files into the new `base/<dbOid>/<new_relfilenode>_pax/`
   directory preserving their original names (`ptblockname`,
   `<ptblockname>.toast`, `<ptvisimapname>`).
3. Insert rows into the new `pg_pax_blocks_<new_relid>` matching the dumped
   metadata. Field values are unchanged; only the aux relation OID differs.
4. Set `pg_pax_fastsequence.seq` to at least `max(ptblockname) + 1` to avoid
   collisions on future inserts.

### Alternative: clean snapshot via CLUSTER

Running `VACUUM FULL` or `CLUSTER` before backup eliminates visimap files,
consolidates external TOAST, and drops orphan files. The result is a clean
`(N, N.toast?)` set with `ptvisimapname = NULL` everywhere. This is the
simplest input to a backup tool, at the cost of a full-table rewrite with an
exclusive lock.

### Consistency gotchas

- The pair `(N, N.toast)` must be backed up together — PORC stripe metadata
  contains the toast offsets.
- `ptvisimapname` is updated atomically with the heap MVCC semantics of the
  aux table. Always pair the visimap file name you copy with the aux-row
  snapshot you took.
- Files are never rewritten in place, so streaming copy without locking is
  safe once the snapshot has captured the file names.

## 9. WAL Logging

PAX uses a custom resource manager to write WAL records for every file-level
operation. Files are recoverable from WAL alone during archive recovery;
crash recovery skips PAX replay because file writes are fsync'd before
transaction commit.

### Custom resource manager

Registered at extension load in `_PG_init`,
[`pax_access_handle.cc:1203`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_handle.cc#L1203):

```cpp
paxc::RegisterPaxRmgr();
```

`PAX_RMGR_ID = 199` (in the user-reserved range), defined in
[`paxc_desc.h`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_desc.h).
The rmgr struct provides `rm_redo` / `rm_desc` / `rm_identify` callbacks via
`RegisterCustomRmgr`,
[`paxc_wal.cc:481-497`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L481-L497).

### Record types

| Record | Site | Payload |
|---|---|---|
| `XLOG_PAX_INSERT` | [`paxc_wal.cc:204`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L204) | RelFileNode + filename + offset + raw bytes |
| `XLOG_PAX_CREATE_DIRECTORY` | [`paxc_wal.cc:240`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L240) | RelFileNode of the new `_pax/` directory |
| `XLOG_PAX_TRUNCATE` | [`paxc_wal.cc:255`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L255) | RelFileNode whose `_pax/` is being wiped |

`XLOG_PAX_INSERT` carries the **actual file bytes**, not just an event, so
data, `.toast`, and `.visimap` files are fully reconstructible from WAL.
Long writes are split into multiple records at increasing offsets; offset 0
implies "create or truncate the file".

Visimap writes share this path —
[`pax.cc:658-669`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L658-L669):

```cpp
visimap_file->WriteN(raw.bitmap, raw.size);
if (need_wal_) {
  cbdb::XLogPaxInsert(rel_->rd_node, visimap_file_name, 0,
                      raw.bitmap, raw.size);
}
visimap_file->Close();
```

### Storage manager (relfilenode placeholder)

PAX also registers a custom storage manager,
[`pax_access_handle.cc:1205`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_handle.cc#L1205):

```cpp
paxc::RegisterPaxSmgr();
```

The relfilenode placeholder itself is created via standard
`RelationCreateStorage(..., SMGR_PAX, ...)`,
[`paxc_wrappers.cc:486`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/comm/paxc_wrappers.cc#L486),
which emits an ordinary `XLOG_SMGR_CREATE`. Individual block files inside
`<relfilenode>_pax/` bypass smgr — they go through `LocalFileSystem`
([`local_file_system.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/local_file_system.cc))
with raw `open(2)` / `write(2)` and are logged via `XLogPaxInsert`.

### Durability and crash recovery

PAX does **not** participate in PostgreSQL's checkpoint sync queue. Durability
is per-transaction: every PAX file is fsync'd before close,
[`local_file_system.cc:168-175`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/local_file_system.cc#L168-L175):

```cpp
void LocalFile::Flush() {
  int rc = fsync(fd_);
  ...
}
```

The ORC writer flushes the data file (and the toast file, when present)
before marking itself closed,
[`orc_writer.cc:939-947`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/orc/orc_writer.cc#L939-L947).
Because file contents are on disk before the aux-row commit, **crash
recovery does not need to replay PAX records** — the redo callback
short-circuits at
[`paxc_wal.cc:435-440`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L435-L440):

```cpp
static void pax_rmgr_redo(XLogReaderState *record) {
  ...
  if (IsCrashRecoveryOnly()) return;
  ...
}
```

Archive recovery (PITR, streaming replication, restoring from a base
backup) does **not** set `IsCrashRecoveryOnly`, so PAX records are fully
applied. `XLogRedoPaxInsert` opens the target file with `O_CREAT | O_TRUNC`
when the record is at offset 0 and writes-at-offset otherwise; if the
parent `_pax/` directory is missing it flags the file invalid rather than
fabricating directory state
([`paxc_wal.cc:271-374`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc#L271-L374)).

### Implications for backup tools

For physical backups that rely on archive recovery (`pg_basebackup`-style,
including WAL-G), the WAL stream alone is sufficient to reconstruct any
PAX file created between `pg_start_backup()` and `pg_stop_backup()`. The
contract matches that of ordinary heap relations:

- Capturing whatever exists in `<relfilenode>_pax/` at any point in the
  backup window is safe — the on-disk snapshot does **not** need to be
  coherent with the aux catalog snapshot.
- The aux catalog (`pg_ext_aux.pg_pax_*`) is plain heap and is covered by
  standard heap WAL, so it replays alongside file contents.
- Files orphaned by aborted transactions in the backup window are
  truncated and reused on the next INSERT to the same `block_id`
  ([`pax.cc:213-224`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc#L213-L224)),
  so including them in the backup is harmless.

The §8 *clean-snapshot via `CLUSTER`* path remains the simplest input for a
**logical** export tool, but is not required for physical backup.

## 10. Source Map

All links pin to the `2.1.0-incubating` tag.

| Area | Source |
|---|---|
| Storage format (PORC) | [`doc/README.format.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.format.md) |
| Catalog | [`doc/README.catalog.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.catalog.md) |
| TOAST | [`doc/README.toast.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/doc/README.toast.md) |
| CTID encoding | [`src/cpp/storage/README_CTID_in_pax.md`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/README_CTID_in_pax.md) |
| Table access method entry points | [`src/cpp/access/pax_access_handle.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_handle.cc) |
| VACUUM / CLUSTER callbacks | [`src/cpp/access/pax_access_method_internal.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_method_internal.cc) |
| DELETE implementation | [`src/cpp/access/pax_deleter.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_deleter.cc) |
| Visimap read | [`src/cpp/access/pax_visimap.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_visimap.cc) |
| Visimap write + scan integration | [`src/cpp/storage/pax.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/pax.cc) |
| Writer | [`src/cpp/storage/orc/orc_writer.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/orc/orc_writer.cc) |
| Aux-table catalog | [`src/cpp/catalog/pax_aux_table.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/catalog/pax_aux_table.cc) |
| File-system abstraction | [`src/cpp/storage/file_system.h`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/file_system.h), [`local_file_system.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/local_file_system.cc) |
| Path building | [`src/cpp/comm/paxc_wrappers.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/comm/paxc_wrappers.cc) |
| Custom WAL rmgr | [`src/cpp/storage/wal/paxc_wal.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc), [`paxc_desc.h`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/wal/paxc_desc.h) |
| Custom storage manager | [`src/cpp/storage/paxc_smgr.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/storage/paxc_smgr.cc) |
| Extension entry point | [`src/cpp/access/pax_access_handle.cc`](https://github.com/apache/cloudberry/blob/2.1.0-incubating/contrib/pax_storage/src/cpp/access/pax_access_handle.cc) (`_PG_init`) |


## 11. wal-g Storage layout


Storage object names are built from `<spc>_<db>_<md5>_<rel>_<filename>_<id>_pax`.
Dots in the source filename (`<id>.toast`, `<id>_<gen>_<xid>.visimap`) are
replaced with `_` so the resulting storage path has no extension that the
extract-side codec lookup would interpret as a compression format.

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