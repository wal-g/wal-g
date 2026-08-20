# Journal objects

A journal records the volume of WAL archived between one backup and the next. It is the same object every WAL-G database keeps, and it knows nothing about Greenplum specifics such as AO/AOCS or PAX storage.

## Storage layout

```
basebackups_005/                                 ← cluster
  backup_<timestamp>_backup_stop_sentinel.json   ← names the backup of every segment
  journal_backup_<timestamp>                     ← WAL volume, whole cluster
segments_005/seg-1/                              ← coordinator
  basebackups_005/journal_base_<...>             ← journal of this segment
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
