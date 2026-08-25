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
	mu                   sync.RWMutex
	lastHandledTS        models.Timestamp
	lastDurableTS        models.Timestamp
	checkpointGeneration uint64
	dirty                bool
}

func NewReplayProgress(since models.Timestamp) *ReplayProgress {
	return &ReplayProgress{lastHandledTS: since, lastDurableTS: since}
}

func (p *ReplayProgress) Snapshot() (models.Timestamp, models.Timestamp, uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastHandledTS, p.lastDurableTS, p.checkpointGeneration
}

func (p *ReplayProgress) ResetAttempt() {
	p.mu.Lock()
	p.lastHandledTS = p.lastDurableTS
	p.dirty = false
	p.mu.Unlock()
}

func (p *ReplayProgress) Initialize(ts models.Timestamp) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.checkpointGeneration == 0 && !p.dirty {
		p.lastHandledTS = ts
		p.lastDurableTS = ts
	}
}

func (p *ReplayProgress) NeedsCheckpoint() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirty
}

func (p *ReplayProgress) setHandled(ts models.Timestamp) {
	p.mu.Lock()
	p.lastHandledTS = ts
	p.dirty = true
	p.mu.Unlock()
}

func (p *ReplayProgress) setDurable(ts models.Timestamp) {
	p.mu.Lock()
	p.lastDurableTS = ts
	p.checkpointGeneration++
	p.dirty = false
	p.mu.Unlock()
}

type CheckpointingApplier struct {
	db            client.MongoDriver
	applier       replayDBApplier
	interval      time.Duration
	progress      *ReplayProgress
	checkpointDue bool
}

type replayDBApplier interface {
	Apply(context.Context, models.Oplog) error
	Close(context.Context) error
	HasPendingTransactions() bool
}

func NewCheckpointingApplier(
	db client.MongoDriver,
	applier replayDBApplier,
	interval time.Duration,
	progress *ReplayProgress,
) *CheckpointingApplier {
	return &CheckpointingApplier{db: db, applier: applier, interval: interval, progress: progress}
}

func (a *CheckpointingApplier) Apply(ctx context.Context, ch chan *models.Oplog) (chan error, error) {
	errC := make(chan error, 1)
	go func() {
		defer close(errC)
		defer func() {
			if err := a.applier.Close(ctx); err != nil {
				select {
				case errC <- fmt.Errorf("can not close applier: %w", err):
				default:
				}
			}
		}()

		timer := time.NewTimer(a.interval)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				errC <- ctx.Err()
				return

			case <-timer.C:
				a.checkpointDue = true
				if !a.applier.HasPendingTransactions() {
					if err := a.makeDurable(ctx); err != nil {
						errC <- err
						return
					}
					a.checkpointDue = false
					timer.Reset(a.interval)
				}

			case op, ok := <-ch:
				if !ok {
					if a.applier.HasPendingTransactions() {
						errC <- fmt.Errorf("oplog stream ended with an incomplete transaction")
						return
					}
					if err := a.makeDurable(ctx); err != nil {
						errC <- err
					}
					return
				}

				if err := a.applier.Apply(ctx, *op); err != nil {
					errC <- fmt.Errorf("can not handle op: %w", err)
					return
				}
				a.progress.setHandled(op.TS)

				if a.checkpointDue && !a.applier.HasPendingTransactions() {
					if err := a.makeDurable(ctx); err != nil {
						errC <- err
						return
					}
					a.checkpointDue = false
					timer.Reset(a.interval)
				}
			}
		}
	}()

	return errC, nil
}

func (a *CheckpointingApplier) makeDurable(ctx context.Context) error {
	handled, _, _ := a.progress.Snapshot()
	if !a.progress.NeedsCheckpoint() {
		return nil
	}
	if err := a.db.Fsync(ctx); err != nil {
		return fmt.Errorf("can not fsync oplog replay at %s: %w", handled, err)
	}
	a.progress.setDurable(handled)
	tracelog.InfoLogger.Printf("Oplog replay progress is durable through %s", handled)
	return nil
}
