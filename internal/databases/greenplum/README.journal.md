# Journal and shared size objects

A journal records the volume of WAL archived between one backup and the next. It is the same object every WAL-G database keeps, and it knows nothing about Greenplum specifics such as AO/AOCS or PAX storage.

The shared size is the second, independent figure: the volume a backup added to the AO/AOCS and PAX storage shared between backups. It is Greenplum-specific and lives in objects of its own.

## Storage layout

```
basebackups_005/                                 ← cluster
  backup_<timestamp>_backup_stop_sentinel.json   ← names the backup of every segment
  journal_backup_<timestamp>                     ← WAL volume, whole cluster
  backup_<timestamp>/{ao,pax}_files_metadata.json ← shared volume, whole cluster
segments_005/seg-1/                              ← coordinator
  basebackups_005/journal_base_<...>             ← journal of this segment
  basebackups_005/{aosegments,paxfiles}/         ← the shared storage itself
  wal_005/                                       ← WAL of this segment
segments_005/seg0/                               ← segment 0, and so on
  ...
```

The cluster-wide journal is the one to read; the per-segment ones are the inputs it is aggregated from.

Unlike the sentinel, ``journal_<backup>`` does not reduce to the backup name, so no delete mode removes it on its own — it has to be deleted by hand, which is what ``DeleteJournalInfo`` is for.

## How it works

A segment knows nothing about the cluster, and the coordinator never looks at the WAL itself, only at what the segments recorded.

Each **segment**, during ``seg backup-push``, measures the WAL it archived into its own ``wal_005/`` and keeps it in its own journal as ``SizeToNextBackup``, exactly like a standalone Postgres would.

The **coordinator**, at the end of the cluster ``backup-push``, walks the segments named in the *previous* backup's sentinel and writes their sum into that backup's journal.

Journals are written only when ``backup-push`` is given ``--count-journals``, and never for permanent backups: those are not expected to be removed and take no part in WAL retention planning.

A partial sum would silently understate the real volume and be indistinguishable from a genuinely small one, so a single unreadable segment journal leaves the previous ``SizeToNextBackup`` untouched rather than overwriting it with a wrong value.

## Fields

* ``PriorBackupEnd`` — completion time of the preceding backup. Re-linked when a backup in the middle is deleted, merging its interval into the previous one.
* ``CurrentBackupEnd`` — completion time of this backup. Orders the ``journal_<backup>`` objects chronologically, which their names alone do not.
* ``SizeToNextBackup`` — bytes of WAL archived between this backup and the **next** one: zero for the newest backup, filled in when the following one is created. Deliberately *not* the volume of this object's own ``(PriorBackupEnd; CurrentBackupEnd]`` — those timestamps describe the interval *before* the backup and only serve to navigate the chain. The two are one step apart, so a recalculation writes its result into the **previous** backup's journal, never into its own.

The volume is measured differently at the two levels. A **segment** sums the storage sizes of the WAL files in its own ``wal_005/`` falling into ``(PriorBackupEnd; CurrentBackupEnd]`` (``JournalFiles``); the **coordinator** ignores both timestamps and adds up what the segments already measured (``UpdateClusterIntervalSize``).

The cluster figure is therefore not a wall-clock window: a segment finishes its ``backup-push`` before the coordinator creates the restore point, so WAL archived in between lands in that segment's *next* interval. Nothing is lost or double counted along the chain, but the total is not the volume archived cluster-wide between two backup finish times.

Sizes come from the storage object sizes, so they reflect compression and encryption — unlike an estimate derived from the LSN difference.

## Shared size

AO/AOCS and PAX files live in storage shared between backups, where an unchanged file is uploaded once and reused later through deduplication.

Each **segment** initially records what its uploaders actually pushed as ``UploadedSharedSize`` in its own ``ao_files_metadata.json`` and ``pax_files_metadata.json`` — a deduplicated file never reaches the uploader and is not counted. The **coordinator** sums the segments into the cluster-level pair as ``SharedSize``.

AO and PAX stay in separate objects all the way up — separate deduplication age limits, separate cleanup passes.

When a backup is deleted, ``cleanupAOSegments`` and ``cleanupPaxFiles`` recompute ownership for the first surviving backup after each deleted block. Segment history is linear: references to one physical ``StoragePath`` form a continuous interval, and a logical file that disappears and later returns gets a new storage path. Therefore a backup owns the files it references that its immediately preceding surviving backup does not. All later backups keep the same predecessor and ownership. Reassignment reads files metadata only for the affected backup and that predecessor; the separate physical cleanup pass scans all surviving metadata to build the set of storage objects that must be retained.

The new ``UploadedSharedSize`` is the sum of those owned files: ``EOF`` from AO metadata and ``Size`` from PAX metadata. Replacing the value instead of adding a delta makes cleanup idempotent. After all segments finish, the coordinator sums their updated values into ``SharedSize`` only for the corresponding affected cluster backups.
