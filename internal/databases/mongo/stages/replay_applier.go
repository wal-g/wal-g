package stages

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type ReplayProgress struct {
	mu                  sync.RWMutex
	checkpoint          ReplayCheckpoint
	processedTS         models.Timestamp
	pendingTransactions bool
}

type ReplayCheckpoint struct {
	OpTime     models.OpTime
	Generation uint64
}

type ReplayStatus struct {
	ProcessedTS         models.Timestamp
	Checkpoint          ReplayCheckpoint
	PendingTransactions bool
}

func NewReplayProgress(since models.Timestamp) *ReplayProgress {
	return &ReplayProgress{
		checkpoint:  ReplayCheckpoint{OpTime: models.OpTime{TS: since}},
		processedTS: since,
	}
}

func (p *ReplayProgress) Snapshot() ReplayCheckpoint {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.checkpoint
}

func (p *ReplayProgress) Initialize(ts models.Timestamp) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkpoint.Generation == 0 {
		p.checkpoint.OpTime = models.OpTime{TS: ts}
		p.processedTS = ts
	}
}

func (p *ReplayProgress) BeginAttempt() {
	p.mu.Lock()
	p.processedTS = p.checkpoint.OpTime.TS
	p.pendingTransactions = false
	p.mu.Unlock()
}

func (p *ReplayProgress) Status() ReplayStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return ReplayStatus{
		ProcessedTS:         p.processedTS,
		Checkpoint:          p.checkpoint,
		PendingTransactions: p.pendingTransactions,
	}
}

func (p *ReplayProgress) MarkProcessed(ts models.Timestamp, pendingTransactions bool) {
	p.mu.Lock()
	p.processedTS = ts
	p.pendingTransactions = pendingTransactions
	p.mu.Unlock()
}

func (p *ReplayProgress) MarkDurable(opTime models.OpTime) {
	p.mu.Lock()
	p.checkpoint.OpTime = opTime
	p.checkpoint.Generation++
	p.mu.Unlock()
}

type CheckpointingApplier struct {
	db              client.MongoDriver
	applier         replayDBApplier
	interval        time.Duration
	batchSize       int
	progress        *ReplayProgress
	checkpointDue   bool
	lastAppliedTS   models.Timestamp
	dirty           bool
	finalizeCatchUp bool
}

type replayDBApplier interface {
	ApplyBatch(context.Context, []models.Oplog) error
	Close(context.Context) error
	HasPendingTransactions() bool
	LastAppliedOpTime() (models.OpTime, bool)
}

const (
	DefaultReplayApplyBatchSize = 50
	// Flush an incomplete batch promptly when the stream is quiet.
	replayApplyBatchMaxWait = 10 * time.Millisecond
	// The command adds BSON array overhead to the raw entries.
	replayApplyBatchBytes = models.MaxDocumentSize / 2
)

type pendingReplayBatch struct {
	operations []models.Oplog
	bytes      int
	timer      *time.Timer
	tick       <-chan time.Time
}

func NewCheckpointingApplier(
	db client.MongoDriver,
	applier replayDBApplier,
	interval time.Duration,
	progress *ReplayProgress,
	batchSize int,
	finalizeCatchUp bool,
) *CheckpointingApplier {
	if batchSize <= 0 {
		batchSize = DefaultReplayApplyBatchSize
	}
	return &CheckpointingApplier{
		db: db, applier: applier, interval: interval, progress: progress,
		batchSize: batchSize, finalizeCatchUp: finalizeCatchUp,
	}
}

func (a *CheckpointingApplier) Apply(ctx context.Context, ch chan *models.Oplog) (chan error, error) {
	errC := make(chan error, 1)
	go func() {
		defer close(errC)
		err := a.run(ctx, ch)
		if closeErr := a.applier.Close(ctx); err == nil && closeErr != nil {
			err = fmt.Errorf("can not close applier: %w", closeErr)
		}
		if err != nil {
			errC <- err
		}
	}()

	return errC, nil
}

func (a *CheckpointingApplier) run(ctx context.Context, ch chan *models.Oplog) error {
	timer := time.NewTimer(a.interval)
	defer timer.Stop()
	batch := pendingReplayBatch{
		operations: make([]models.Oplog, 0, min(a.batchSize, DefaultReplayApplyBatchSize)),
		timer:      time.NewTimer(replayApplyBatchMaxWait),
	}
	defer batch.timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			a.checkpointDue = true
			if err := a.flushBatch(ctx, &batch, timer); err != nil {
				return err
			}
			if a.checkpointDue && !a.applier.HasPendingTransactions() {
				if err := a.checkpoint(ctx, timer); err != nil {
					return err
				}
			}
		case <-batch.tick:
			if err := a.flushBatch(ctx, &batch, timer); err != nil {
				return err
			}
		case op, ok := <-ch:
			if !ok {
				if err := a.flushBatch(ctx, &batch, timer); err != nil {
					return err
				}
				return a.finish(ctx)
			}
			if err := a.appendOperation(ctx, &batch, op, timer); err != nil {
				return err
			}
		}
	}
}

func (a *CheckpointingApplier) appendOperation(
	ctx context.Context,
	batch *pendingReplayBatch,
	op *models.Oplog,
	checkpointTimer *time.Timer,
) error {
	if len(batch.operations) > 0 && batch.bytes+len(op.Data) > replayApplyBatchBytes {
		if err := a.flushBatch(ctx, batch, checkpointTimer); err != nil {
			return err
		}
	}
	if len(batch.operations) == 0 {
		batch.timer.Reset(replayApplyBatchMaxWait)
		batch.tick = batch.timer.C
	}
	batch.operations = append(batch.operations, *op)
	batch.bytes += len(op.Data)
	if len(batch.operations) == a.batchSize {
		return a.flushBatch(ctx, batch, checkpointTimer)
	}
	return nil
}

func (a *CheckpointingApplier) flushBatch(
	ctx context.Context,
	batch *pendingReplayBatch,
	checkpointTimer *time.Timer,
) error {
	if len(batch.operations) == 0 {
		return nil
	}
	batch.timer.Stop()
	batch.tick = nil
	if err := a.applyOperations(ctx, batch.operations, checkpointTimer); err != nil {
		return err
	}
	batch.operations = batch.operations[:0]
	batch.bytes = 0
	return nil
}

func (a *CheckpointingApplier) applyOperations(
	ctx context.Context,
	ops []models.Oplog,
	timer *time.Timer,
) error {
	if err := a.applier.ApplyBatch(ctx, ops); err != nil {
		return fmt.Errorf("can not handle op: %w", err)
	}
	a.lastAppliedTS = ops[len(ops)-1].TS
	a.progress.MarkProcessed(a.lastAppliedTS, a.applier.HasPendingTransactions())
	a.dirty = true
	if !a.checkpointDue || a.applier.HasPendingTransactions() {
		return nil
	}
	return a.checkpoint(ctx, timer)
}

func (a *CheckpointingApplier) checkpoint(ctx context.Context, timer *time.Timer) error {
	if err := a.makeDurable(ctx); err != nil {
		return err
	}
	if err := replayCheckpointTestBarrier(ctx, a.progress); err != nil {
		return err
	}
	a.checkpointDue = false
	timer.Reset(a.interval)
	return nil
}

func (a *CheckpointingApplier) finish(ctx context.Context) error {
	if a.applier.HasPendingTransactions() {
		return fmt.Errorf("oplog stream ended with an incomplete transaction")
	}
	return a.makeDurable(ctx)
}

func (a *CheckpointingApplier) makeDurable(ctx context.Context) error {
	if !a.dirty {
		return nil
	}
	opTime, ok := a.applier.LastAppliedOpTime()
	if !ok || !models.Equal(opTime.TS, a.lastAppliedTS) {
		opTime = models.OpTime{TS: a.lastAppliedTS}
	}
	if a.finalizeCatchUp {
		if err := a.db.ChangeOplogLastTimestamp(ctx, opTime); err != nil {
			return fmt.Errorf("can not finalize oplog replay checkpoint at %s: %w", a.lastAppliedTS, err)
		}
	} else if err := a.db.Fsync(ctx); err != nil {
		return fmt.Errorf("can not fsync oplog replay at %s: %w", a.lastAppliedTS, err)
	}
	a.progress.MarkDurable(opTime)
	a.dirty = false
	tracelog.InfoLogger.Printf("Oplog replay progress is durable through %s", a.lastAppliedTS)
	return nil
}
