# WAL summary incremental backup PoC

On a PostgreSQL 18+ primary, set `summarize_wal=on`. Configure WAL-G with
`WALG_USE_WAL_SUMMARY=true`, `WALG_DELTA_MAX_STEPS=3` and `WALG_DELTA_ORIGIN=LATEST`,
then run `wal-g backup-push "$PGDATA"` normally. Restore with `backup-fetch` as
usual: this changes block discovery, not WAL-G's incremental backup format.

Before an incremental backup, WAL-G waits up to a minute for the summarizer,
archives the required summaries with normal compression/encryption under
`wal_sum_005/` beside `wal_005/`, and reads their block maps from storage.
Previously archived summaries can fill holes in local retention. Missing ranges,
invalid checksums and timeline changes fail explicitly. Directory metadata is
still walked to determine backup contents, but unchanged relation pages are not
scanned. Recreated/truncated relations and newly created databases are copied
fully; modification times do not override the summary map.

`wal-g wal-summary-push "$PGDATA/pg_wal/summaries/FILE.summary"` can also archive a
published summary independently, before PostgreSQL removes it. This PoC adds no
background summary archiver or automatic archive retention; retain the summaries
needed from the predecessor backup's start LSN. Only same-timeline local backups
with the regular tar composer are supported; `WALG_USE_WAL_DELTA` is incompatible.

The Docker scenario creates a full backup and two encrypted/compressed deltas,
removes local summaries to exercise archive reuse, checks failure on missing
history and restores the database:

```sh
make PG_MAJOR=18 TEST=pg_wal_summary_test pg_integration_test
```
