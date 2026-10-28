# Working on WAL-G

Keep changes narrow and preserve backup/restore compatibility. These notes are
review aids, not a substitute for reading the code on the branch being changed.

## Keep these notes useful

- When a task reveals a non-obvious, reusable invariant or failure mode, add or
  revise a short note here with a code pointer or regression test. Prefer a test
  for behavior that can be tested. Include the documentation change in review.
- Recheck relevant notes against the current branch; correct or remove obsolete
  claims instead of accumulating contradictory advice. Distinguish implemented
  behavior, a reproduced bug, and a proposed design.
- Do not record session transcripts, temporary paths, credentials, incident
  details, or transient CI status. Put substantial design discussions in docs
  or an issue/PR and link them. Documentation does not authorize unrelated
  changes, publishing, merging, or deleting data.

## Where to look

- `cmd/<database>` and `main/<database>`: command wiring and binaries.
- `internal/databases/<database>`: database-specific backup, restore and WAL
  handling. PostgreSQL code is also used by Greenplum segment operations.
- `pkg/storages/storage`, backend packages and `internal/multistorage`: storage
  contracts, implementations, destination selection and operation policies.
- `internal/daemon`, PostgreSQL's `daemon_handler.go`, and `internal/asm`:
  daemon protocol, command execution and archive-status bookkeeping.
- `Makefile`, `.github/workflows`, `docker` and `tests_func`: build/test setup.
  Read the current Go version, build tags and experiment settings there; do not
  copy a toolchain recipe from an old release. `cmd/pg/exporter` has its own module.

## PostgreSQL backup identity and publication

- A backup exists for listing/restoration when its stop sentinel exists; a
  directory or tar object alone is not a completed backup. See
  `internal/backup.go` (`SentinelExists`, `CheckExistence`, `UploadSentinel`).
- Absence of a sentinel does not prove that a writer has stopped. Files may be
  an in-progress attempt or abandoned garbage. Cleanup and reservation ownership
  need separate reasoning from backup visibility.
- Backup names are not unique attempt IDs. Replicas sharing a restartpoint can
  produce the same name. Review both local and streaming paths in
  `backup_push_handler.go`, `query_runner.go` and `streaming_base_backup.go`.
- Reusing an incomplete backup's name can mix old and new objects. Restore lists
  `tar_partitions` (`backup.go:GetTarNames`, `tars_to_extract_provider.go`);
  metadata filtering is not unconditional, and backups without file metadata
  are supported. Do not assume a new sentinel makes stale tar files harmless.
- Empty directories are not payload. SeaweedFS 4.40 can return a CommonPrefix
  after its last object has been deleted. If checking for leftover data, inspect
  actual objects within the exact backup prefix, including zero-byte objects;
  fail on inspection errors rather than treating them as an empty listing.

### Reservation/deletion design under discussion

[Backup-name reservation (#2707)](https://github.com/wal-g/wal-g/pull/2707)
is separate from sentinel-based publication. Check which parts your branch
actually implements. A HEAD/Exists followed by PUT is not an exclusive claim.

The proposed deletion ordering is: remove the sentinel and confirm success,
remove the payload and confirm success, then release the reservation. This is
not guaranteed by the current shared `internal/delete_handler.go`, which passes
selected objects together to `DeleteObjects`. Sorting one batch is not a
transaction or a barrier between these phases. Safe release also requires
excluding stale writers and overlapping cleanup; a missing sentinel or an old
timestamp alone cannot establish that. Reservation release remains a separate
protocol change, not a consequence of ignoring empty directories.
Removing a sentinel also does not protect a restore that has already read it.

## Storage and failure semantics

- Preserve the selected destination and its prefix through wrappers. A guarantee
  on one storage is not a distributed guarantee across failover destinations.
  Inspect `internal/multistorage/folder.go`, its policies and stats collector;
  do not infer mid-operation failover from the configuration's name.
- Cancellation or a lost response does not imply that an object was not written.
  Test ambiguous success and retries; avoid silently releasing ownership or
  retrying on another destination after an exclusivity conflict.
- Keep per-storage endpoint discovery, credentials and HTTP transport changes
  isolated. Changing a shared transport can affect an already active primary
  client. Check `pkg/storages/s3/session.go` and its tests.
- Recursive filesystem listings can contain leading slashes. Normalize paths
  before applying protected-prefix filters, and test both forms through wrappers.

## Avoid listings in hot paths

- Design around direct object access rather than directory enumeration (`ls`,
  `ReadDir`) or S3 LIST. Directories and prefixes can contain thousands or
  millions of entries. Local `pg_wal` enumeration has been observed to become
  the main archival bottleneck; local filesystem access is not automatically
  cheap. Review scan frequency in `bguploader.go` as well as storage LIST calls.
- Prefer deriving the next WAL filename or numbered tar name from the format,
  or using recorded metadata, instead of listing to discover predictable names.
  Respect timeline/segment-size rules and the actual tar naming scheme; a
  missing predicted object must not silently hide gaps or late arrivals.
- For every necessary listing, state the approximate expected entry count,
  prefix/directory scope and frequency. Bound traversal and memory use, stop
  early when possible, and avoid rescanning per WAL or per object. Pagination
  alone does not make a full scan of millions of keys inexpensive.
- Listings can still be appropriate, for example to verify that deletion left
  no unexpected objects. Keep such checks scoped to the affected backup prefix,
  outside the steady-state hot path where possible, and account for their cost.

## WAL pipelines

- Checksum retries must replace the bytes entering the archive, not just
  recheck a page after its original bytes have been copied. Preserve `pd_checksum`
  during verification; incremental rereads use original relation-file block
  offsets, including relation segment offsets for checksum calculation. See
  `checksum_retry_reader_test.go` (retry limits, archive bytes and metadata,
  incremental offsets, truncation, cancellation and replacement).

- Keep one `pgproto3.Backend` per `wal-send` connection, including startup and
  COPY: it can buffer the next query while reading CopyDone. Hand reading back
  to the command loop only after the COPY reader exits, and serialize each
  `Send`/`Flush` batch with `writeMu`. Preserve `SetMaxBodyLen` when constructing
  the backend. See `wal_send_protocol_test.go`.
- Validate `wal-send` with stock PostgreSQL clients as well as `pglogrepl`.
  `pg_receivewal` requests `SHOW data_directory_mode` and `SHOW wal_segment_size`,
  and physical `IDENTIFY_SYSTEM` needs a SQL NULL `dbname`, not an empty string.
  See `wal_send_handler.go:handleQuery`, `handleIdentifySystem` and the opt-in
  stock-client/standby tests in `wal_send_postgres_test.go`.
- Physical `XLogData` has no timeline field. A timeline transition requires
  ending COPY and reporting the successor timeline and switch LSN; changing the
  storage filename cannot notify the receiver. Validate history and the fork
  position before selecting a successor (`wal_send_handler.go:run`).
- Live timeline discovery cannot require an old-timeline `.partial`: promotion
  at a segment boundary creates none. While waiting for missing WAL, refresh
  archive identity through the shared cache every 12 polling intervals; keep
  this slower than individual WAL polls. See `wal_send_regression_test.go`
  (`TestWalSendLiveTimelineSwitchWithoutPartial`,
  `TestWalSendIdleTimelineDiscoverySharesRefresh`).
- A WAL-sized decoded payload can still end with a checksum/read error. Drain
  the decoder to EOF before releasing the final streaming chunk. Serialize late
  prefetch publication with foreground cleanup so consumed files cannot reappear;
  see `wal_send_regression_test.go`.
- `pg_walfile_name()` maps an exact segment boundary to the preceding file.
  Archive fixtures that exclude the active WAL must use the insertion LSN + 1
  and omit future/preallocated files, or they may drop the just-switched segment.
  Perform that addition in Go: PostgreSQL 10 lacks SQL `pg_lsn + integer`.
  See `wal_send_cli_test.go` and `wal_send_postgres_test.go`.
- Promotion may finish before its checkpoint recycles old WAL. In standby test
  fixtures, finish that checkpoint before generating the new WAL to archive;
  otherwise `pg_wal` entries can disappear between `ReadDir` and `ReadFile`.
  See `wal_send_postgres_test.go:TestWalSendRealPostgresStandby`.
- Distinguish foreground WAL completion, background work, metadata flushes and
  acknowledgment to PostgreSQL. Trace standalone and daemon paths separately;
  a client timeout is not evidence that all server-side work was cancelled.
- `WALG_UPLOAD_CONCURRENCY` and `TOTAL_BG_UPLOADED_LIMIT` are different controls.
  The latter is not a worker count. Its precise cycle/lookahead semantics vary
  between pipeline revisions; verify `wal_push_handler.go`, `bguploader.go` and
  the daemon implementation on the target branch. Raising it does not by itself
  prove autonomous backlog draining.
- For idle-backlog bugs, test a blocked foreground upload, a blocked background
  upload, more ready WALs than the discovery window, and no further client calls.
  CPU waves alone do not identify a scheduler bug or a storage bottleneck.
- Publish prefetched files only after successful download/decompression and
  finalization. On upload errors, close source files and compression pipe readers
  so producers cannot remain blocked. See `prefetch.go`, `uploader.go`,
  `compress_and_encrypt.go` and their failure-path tests.

## Validation and handoff

- Start with a failing reproducer; run focused tests, affected package suites and
  race tests for concurrency changes. Use the current Makefile/CI recipes for
  native dependencies and integration tests. Include Greenplum where shared
  PostgreSQL behavior changes.
- PostgreSQL package test initializers export Viper defaults to the environment.
  CLI integration tests must isolate inherited configuration variables; otherwise
  defaults such as `WALG_PG_WAL_SIZE=16` override an explicit 64 MiB config file.
  See `wal_send_cli_test.go:walSendCLIStandby`.
- A compiled Go test binary still runs all package initializers with `-test.run`.
  PostgreSQL's pagefile fixtures load `../../../test/testdata` before any test;
  ship those files with the binary and preserve its package-relative working
  directory. `docker/pg_tests/Dockerfile_prefix` checks that runtime layout with
  `postgres.test -test.run='^$'` during the image build.
- Check Windows behavior rather than assuming Unix error identities. Cross-
  compilation does not execute Windows tests.
- Report what actually ran: mocked SDK calls, a real storage service, a real
  PostgreSQL restore and the Docker matrix are different levels of evidence.
  Local green tests do not mean CI is green or a production rollout is proven.
- For backports, inspect the deployed base and dependencies. Do not import a
  newer SDK or unrelated defaults to resolve a cherry-pick conflict. Preserve
  authorship and keep independent fixes independently reviewable.
