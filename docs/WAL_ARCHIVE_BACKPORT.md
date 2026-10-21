# WAL archival backport for build and review

Base: deployed `36138cabcc04e087a3cc35fb1a372ccf93869134`, followed by
`7e797ffbd2058459bada4b76543efa5043e1242a` (secondary-only S3 discovery/transport
isolation, SDK v1). No SDK or dependency upgrade. Build using the deployed
packaging recipe with this branch as the source revision and a distinct version.

## Changes

- Daemon WAL uploads share a bounded pipeline across archive commands. A reply
  waits for the requested WAL and its metadata, not unrelated background WALs.
- After the first request establishes the WAL directory, a background cursor
  keeps advancing independently of client requests. Worker completion refills
  the pipeline. `TOTAL_BG_UPLOADED_LIMIT=32` bounds candidate batches to 31;
  it no longer limits total progress beyond the last requested WAL. Values <= 1
  still disable background uploads; upload concurrency is unchanged.
- Upload concurrency remains bounded, including after client timeout. Each job
  has its own daemon upload timeout. Storage is selected anew per job through the
  existing multi-storage health cache. In-flight jobs do not switch storage.
- Contiguous WAL names are predicted without directory listing. Streaming
  directory passes handle gaps and rediscover failed/late ready files behind
  the cursor. Entries are read in bounded chunks without sorting the directory.
  Passes are separated by at least five seconds; candidates are retained while
  workers are busy. Retry delays expire even when the client is stuck.
- Early upload failure closes the compression pipe and WAL file. Missing ASM
  markers after a write failure do not cause an immediate re-upload loop.

No `wal-fetch`/prefetch changes. Standalone `wal-push` and WAL delta mode retain
their existing batch lifecycle. Duplicate uploads remain possible; correctness
does not depend on exactly-once execution. A storage error never acknowledges a
failed WAL. Successful WAL bytes remain cached when metadata needs a retry.

## Local validation

Latest validation (2026-10-21): macOS/arm64, Go 1.26.2 with
`GOEXPERIMENT=jsonv2`, Brotli enabled. Tests use `-vet=off`; `go.mod` is unchanged.
Use a short `TMPDIR` for Unix-socket tests.

```sh
go test -tags brotli -vet=off -race -count=3 -timeout=120s \
  ./internal/databases/postgres \
  -run '^TestDaemonArchiv|^TestServe_'

go test -tags brotli -vet=off -count=1 -timeout=180s \
  ./internal/databases/postgres ./internal ./internal/daemon ./internal/asm \
  ./internal/multistorage/... ./pkg/storages/s3

go build -tags brotli ./main/pg
```

Regression coverage includes blocked background uploads, ASM-hit refill,
bounded concurrency after cancellation, failures/retries, per-job timeout,
restart markers, metadata failure, partial WAL marker safety, gap/rollover
lookahead, storage reselection, no-alive-storage errors, and daemon/client
communication over a real Unix socket with compressed payload verification.
Uploader tests inject SDK-v1 S3 503/SlowDown responses and an early bandwidth-
limiter deadline, then check producer termination and recovery after S3 recovers.

All commands above passed in the latest run, including the complete PostgreSQL
test package (earlier Go 1.27.2 runs timed out in backup tests; those paths were
not changed here). A new regression holds the foreground and one background WAL
blocked while another worker drains the remaining 5003 of 5005 ready files,
with a discovery batch of 1023 and no further client requests. Both contiguous
and sparse names pass. The same contiguous test fails on the previous backport
`f5f0524a`. Tests also cover autonomous retries, late ready files, new contiguous
arrivals, bounded candidate batches, listing cleanup and retry-state expiry.

The upstream follow-up passed golangci-lint, the full PostgreSQL unit package,
race tests and Windows/amd64 test cross-compilation. This reproduces the scheduler
limitation, not the production CPU-wave root cause or a throughput improvement.

This is a build/review candidate, not a production rollout approval. Linux
packaging, real PostgreSQL archiving, and real S3 failover/load tests remain to
be run. Docker integration recipes were not run locally for this follow-up.

## Upstream landing plan

The independent-progress fix is [PR #2716](https://github.com/wal-g/wal-g/pull/2716),
stacked on [#2697](https://github.com/wal-g/wal-g/pull/2697), not on master.

1. Resource cleanup fixes [#2694](https://github.com/wal-g/wal-g/pull/2694)
   (compression pipe) and [#2695](https://github.com/wal-g/wal-g/pull/2695)
   (WAL file), and S3 isolation [#2668](https://github.com/wal-g/wal-g/pull/2668)
   are already merged. Their deployed-base equivalents are already in this
   branch. Do not cherry-pick the master SDK implementation into this backport.
2. Finish review of #2697 (shared daemon pipeline). Its current head has an
   approval and green CI, but remains draft. Bring it up to date with master
   and rerun CI before merging; do not treat the old CI result as validation
   of a changed head.
3. Review #2716 separately. After #2697 merges, retarget #2716 to master. If the
   parent was squash-merged, rebase only the follow-up commit onto master to
   avoid reintroducing the parent's commits. Verify that the diff still contains
   only this follow-up, rerun CI, then merge. Ship the daemon changes together.
4. [#2698](https://github.com/wal-g/wal-g/pull/2698) optimizes standalone/legacy
   batch discovery and is independent of #2697/#2716. It also has an approval
   and green CI but remains draft pending the stated performance validation.
   It is not included in this archival backport.
5. [#2696](https://github.com/wal-g/wal-g/pull/2696) (prefetch integrity) and
   [#2699](https://github.com/wal-g/wal-g/pull/2699) (portable Windows test) are
   already merged; neither is a new daemon-progress dependency.

Statuses above were checked on 2026-10-21. No PR was merged as part of this task.

## Before rollout

On a staging host, validate normal archiving, slow/throttled S3, primary outage
with a working secondary, all storages unavailable, and daemon restart with
outstanding background uploads. Check that PostgreSQL only acknowledges
successfully stored WALs and that its backlog drains after recovery. Compare
archive throughput, oldest unarchived WAL age, CPU/memory/FD usage, errors and
storage traffic. Keep the previous package available for rollback.

Specifically create a backlog larger than `TOTAL_BG_UPLOADED_LIMIT`, delay one
requested upload, and verify that other workers continue past that boundary.
Repeat with injected upload failures, late ready files and daemon restart; make
sure failed WALs are revisited and no request receives a false success. Record
goroutine stacks and storage latency/retries during CPU troughs to distinguish
idle scheduling from workers blocked on S3. Restart the daemon after replacing
the binary/configuration. No new settings are required; keep upload concurrency
unchanged initially and compare identical loads before tuning batch size.
