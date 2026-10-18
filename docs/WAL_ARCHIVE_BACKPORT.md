# WAL archival backport for build and review

Base: deployed `36138cabcc04e087a3cc35fb1a372ccf93869134`, followed by
`7e797ffbd2058459bada4b76543efa5043e1242a` (secondary-only S3 discovery/transport
isolation, SDK v1). No SDK or dependency upgrade. Build using the deployed
packaging recipe with this branch as the source revision and a distinct version.

## Changes

- Daemon WAL uploads share a bounded pipeline across archive commands. A reply
  waits for the requested WAL and its metadata, not unrelated background WALs.
- Acknowledging an ASM-cached WAL advances the lookahead window. Worker completion
  also refills the pipeline. `TOTAL_BG_UPLOADED_LIMIT=32` means the current WAL
  plus up to 31 successors; it is not the worker count.
- Upload concurrency remains bounded, including after client timeout. Each job
  has its own daemon upload timeout. Storage is selected anew per job through the
  existing multi-storage health cache. In-flight jobs do not switch storage.
- Contiguous WAL names are predicted without directory listing. A bounded cached
  listing handles gaps; full listings are throttled to once per five seconds.
- Early upload failure closes the compression pipe and WAL file. Missing ASM
  markers after a write failure do not cause an immediate re-upload loop.

No `wal-fetch`/prefetch changes. Standalone `wal-push` and WAL delta mode retain
their existing batch lifecycle. Duplicate uploads remain possible; correctness
does not depend on exactly-once execution. A storage error never acknowledges a
failed WAL. Successful WAL bytes remain cached when metadata needs a retry.

## Local validation

Environment: macOS/arm64, Go 1.27.2 with `GOEXPERIMENT=jsonv2`, Brotli enabled.
The older module's json/v2 usage trips the newer toolchain's version vet check;
local test commands use `-vet=off`, without changing `go.mod`.

```sh
go test -tags brotli -vet=off -race -count=20 -timeout=60s \
  ./internal/databases/postgres \
  -run '^TestDaemonArchiver|^TestUploadWALFileCloses|^TestServe_'

go test -tags brotli -vet=off -race -count=1 \
  ./internal ./internal/daemon ./internal/asm \
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

The complete PostgreSQL test package is **not green** locally: backup
walk/composer/extraction tests did not complete within the local time limits.
`TestWalk_DatabaseComposer` also times out on the unchanged deployed base.
These backup paths were not modified or repaired as part of this backport.

This is a build/review candidate, not a production rollout approval. Linux
packaging, real PostgreSQL archiving, and real S3 failover/load tests remain to
be run. No Docker runtime was available for the existing integration recipes.

## Before rollout

On a staging host, validate normal archiving, slow/throttled S3, primary outage
with a working secondary, all storages unavailable, and daemon restart with
outstanding background uploads. Check that PostgreSQL only acknowledges
successfully stored WALs and that its backlog drains after recovery. Compare
archive throughput, oldest unarchived WAL age, CPU/memory/FD usage, errors and
storage traffic. Keep the previous package available for rollback.
