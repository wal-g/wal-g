# Journal objects

## Storage layout

A journal is a small JSON object named after the backup it belongs to, ``journal_<backup>``, stored next to the backup sentinels. Every segment maintains a journal of its own, and the coordinator maintains a cluster-wide one summed from them:

```
basebackups_005/
  backup_<timestamp>_sentinel.json
  journal_backup_<timestamp>                    ← whole cluster
segments_005/seg-1/
  basebackups_005/journal_base_<...>            ← coordinator
  wal_005/                                      ← WAL measured by SizeToNextBackup
  aosegments/, paxfiles/                        ← shared storage measured by SharedSize
segments_005/seg0/
  basebackups_005/journal_base_<...>            ← segment 0
  wal_005/
  aosegments/, paxfiles/
```

The cluster-wide journal is the one to read; the per-segment ones are the inputs it is summed from.

## Fields

| Field | Meaning |
| --- | --- |
| ``PriorBackupEnd`` | Completion time of the backup preceding this one. Points at the previous backup, and is re-linked when a backup in the middle is deleted, so that the deleted interval is merged into the previous one. |
| ``CurrentBackupEnd`` | Completion time of this backup. Orders the ``journal_<backup>`` objects chronologically — the names alone do not — which is how the previous and the next journal are located. |
| ``SizeToNextBackup`` | Bytes of WAL archived between this backup and the next one. Belongs to the interval *after* the backup, so it is zero for the newest one and is filled in when the following backup is created. |
| ``SharedSize`` | Bytes this backup added to the shared AO/AOCS and PAX storage. Describes the backup itself rather than an interval, so it is known immediately. Omitted when zero, and absent altogether for databases without shared storage. |

``SizeToNextBackup`` is **not** the volume of the ``(PriorBackupEnd; CurrentBackupEnd]`` interval of the same object. Those two timestamps describe the interval *before* the backup and exist to navigate the chain of journals — to find the previous backup and to re-link the chain around a deleted one. ``SizeToNextBackup`` covers the interval *after* the backup instead, the one that ends at the next backup.

The two are one step apart: when a journal is recalculated, the WAL falling into that journal's ``(PriorBackupEnd; CurrentBackupEnd]`` is summed and written into the **previous** backup's ``SizeToNextBackup``, never into its own.

Sizes come from the storage object sizes, so they reflect compression and encryption — unlike an estimate derived from the LSN difference.

## TODO: make ``SharedSize`` exact

Every shared object is charged to the backup that uploaded it, so adding ``SharedSize`` over all backups gets close to the total size of the shared storage — but only close. The gap comes from deletion: when a backup is deleted, its ``SharedSize`` disappears with it, even though some of the files it uploaded are still in use by newer backups. Those bytes now belong to no backup, and the total drifts below the real size.

The drift stays bounded while ``WALG_GP_AOSEG_DEDUPLICATION_AGE_LIMIT`` and ``WALG_GP_PAXFILE_DEDUPLICATION_AGE_LIMIT`` are comparable to how long backups are kept. Set the limits much higher than the retention period and the totals drift low.