# Journal and shared size objects

Two independent figures are recorded per backup: the volume of WAL archived between one backup and the next (the **journal**), and the volume a backup added to the storage shared between backups (the **shared size**). They are separate objects. The journal is the WAL-only object every WAL-G database keeps and knows nothing about AO/AOCS or PAX; the shared size is Greenplum-specific.

## Storage layout

```
basebackups_005/                                 ← cluster
  backup_<timestamp>_backup_stop_sentinel.json   ← names the backup of every segment
  journal_backup_<timestamp>                     ← WAL volume, whole cluster
  backup_<timestamp>/
    ao_files_metadata.json                       ← shared AO/AOCS volume, whole cluster
    pax_files_metadata.json                      ← shared PAX volume, whole cluster
segments_005/seg-1/                              ← coordinator
  basebackups_005/
    journal_base_<...>                           ← journal of this segment
    base_<...>/ao_files_metadata.json            ← this segment's volume, plus its file list
    base_<...>/pax_files_metadata.json
    aosegments/, paxfiles/                       ← the shared storage itself
  wal_005/                                       ← WAL of this segment
segments_005/seg0/                               ← segment 0, and so on
  ...
```

The two cluster-level files bear the names the segments use, one folder level up, but they carry a single number each and nothing else: there is no shared file at the cluster level to list, and the cluster uploads nothing of its own. Same name, different content — what tells them apart is the folder they are read from, and nothing ever reads one as the other. They and the cluster journal are the ones to read; the per-segment objects are the inputs they are aggregated from.

Nothing needs to delete the cluster-level files explicitly. ``basebackups_005/backup_<ts>/ao_files_metadata.json`` and ``basebackups_005/backup_<ts>_backup_stop_sentinel.json`` reduce to the same backup name, so every delete mode already removes them together with the backup. The journal does not reduce that way, which is why it is deleted by hand.

## Journals

A segment knows nothing about the cluster, and the coordinator never looks at the WAL itself, only at what the segments recorded.

Each **segment**, during ``seg backup-push``, measures the WAL it archived into its own ``wal_005/`` and keeps it in its own journal as ``SizeToNextBackup``, exactly like a standalone Postgres would.

The **coordinator**, at the end of the cluster ``backup-push``, walks the segments named in the *previous* backup's sentinel and writes their sum into that backup's journal.

Journals are written only when ``backup-push`` is given ``--count-journals``, and never for permanent backups: those are not expected to be removed and take no part in WAL retention planning.

A partial sum would silently understate the real volume and be indistinguishable from a genuinely small one, so a single unreadable segment journal leaves the previous ``SizeToNextBackup`` untouched rather than overwriting it with a wrong value.

### Fields

| Field | Meaning |
| --- | --- |
| ``PriorBackupEnd`` | Completion time of the backup preceding this one. Points at the previous backup, and is re-linked when a backup in the middle is deleted, so that the deleted interval is merged into the previous one. |
| ``CurrentBackupEnd`` | Completion time of this backup. Orders the ``journal_<backup>`` objects chronologically — the names alone do not — which is how the previous and the next journal are located. |
| ``SizeToNextBackup`` | Bytes of WAL archived between this backup and the next one. Belongs to the interval *after* the backup, so it is zero for the newest one and is filled in when the following backup is created. |

``SizeToNextBackup`` is **not** the volume of the ``(PriorBackupEnd; CurrentBackupEnd]`` interval of the same object. Those two timestamps describe the interval *before* the backup and exist to navigate the chain of journals — to find the previous backup and to re-link the chain around a deleted one. ``SizeToNextBackup`` covers the interval *after* the backup instead, the one that ends at the next backup.

The two are one step apart: when a journal is recalculated, the result is written into the **previous** backup's ``SizeToNextBackup``, never into its own.

How the volume is obtained differs by level. A segment sums the storage sizes of the WAL files in its own ``wal_005/`` whose timestamps fall into ``(PriorBackupEnd; CurrentBackupEnd]``. The coordinator ignores both timestamps and simply adds up what the segments have already measured for themselves.

That aggregate is deliberately not a wall-clock window. A segment finishes its ``backup-push`` before the coordinator creates the restore point, so WAL archived in between belongs to that segment's *next* interval. Nothing is lost or double counted along the chain, but the cluster figure is not the volume archived cluster-wide between two backup finish times.

Sizes come from the storage object sizes, so they reflect compression and encryption — unlike an estimate derived from the LSN difference.

## Shared size

AO/AOCS and PAX files live in storage shared between backups, ``aosegments/`` and ``paxfiles/`` under each segment, where an unchanged file is uploaded once and then reused by later backups through deduplication.

Each **segment**, during ``seg backup-push``, counts the bytes its AO and PAX uploaders actually pushed — a deduplicated file never reaches the uploader and is therefore not counted — and records them as ``UploadedSharedSize`` in its own ``ao_files_metadata.json`` and ``pax_files_metadata.json``, next to the file lists those objects already carry.

The **coordinator**, at the end of the cluster ``backup-push``, reads the two metadata objects of every segment named in the sentinel and writes the two sums into the cluster-level pair as ``SharedSize``. AO and PAX stay in separate objects all the way up: they are separate subsystems with separate deduplication age limits and separate cleanup passes, and keeping them apart means each pass writes its own object without a read-modify-write on a shared one.

## TODO: make ``SharedSize`` exact

Today ``SharedSize`` is simply set to the sum of the segments' ``UploadedSharedSize`` and never revisited, so the sum over all backups only approximates the size of the shared storage. The gap comes from deletion: when a backup is deleted, its ``SharedSize`` disappears with it, even though some of the files it uploaded are still in use by newer backups. Those bytes now belong to no backup, and the total drifts below the real size.

The drift stays bounded while ``WALG_GP_AOSEG_DEDUPLICATION_AGE_LIMIT`` and ``WALG_GP_PAXFILE_DEDUPLICATION_AGE_LIMIT`` are comparable to how long backups are kept, since a file is then re-uploaded before its owning backup is deleted. Set the limits much higher than the retention period and the totals drift low.