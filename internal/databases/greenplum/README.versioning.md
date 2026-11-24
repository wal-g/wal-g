# S3 Object Lock for Greenplum backups

Enable versioning and Object Lock on the bucket. Grant the uploader
`s3:PutObjectRetention`. Set a 7-day base retention on the coordinator and segments, including WAL archiving.

```yaml
S3_RETENTION_MODE: COMPLIANCE
S3_RETENTION_PERIOD: 604800  # seconds
```

| Object | Compliance retention from upload |
| --- | --- |
| Heap data | `S3_RETENTION_PERIOD` |
| Sentinels and other backup metadata | `S3_RETENTION_PERIOD` |
| Segment manifests (`ao_files_metadata.json`, `pax_files_metadata.json`) | `S3_RETENTION_PERIOD` |
| WAL segments | `S3_RETENTION_PERIOD` |
| Timeline history files (`.history`) | `S3_RETENTION_PERIOD` |
| Restore points | `S3_RETENTION_PERIOD` |
| Shared AO data in `aosegments/` | `S3_RETENTION_PERIOD` + 1 day + `WALG_GP_AOSEG_DEDUPLICATION_AGE_LIMIT` |
| Shared PAX data in `paxfiles/` | `S3_RETENTION_PERIOD` + 1 day + `WALG_GP_PAXFILE_DEDUPLICATION_AGE_LIMIT` |
| Cluster-level `ao_files_metadata.json` and `pax_files_metadata.json` (shared-size accounting) | `S3_RETENTION_PERIOD` |
| Cluster and segment WAL-size journals (`basebackups_005/journal_<backup>`) | No explicit WAL-G retention |

These periods apply when WAL-G uploads a new version with explicit compliance retention.
For size accounting and segment manifests, see [shared size objects](README.journal.md#shared-size).
