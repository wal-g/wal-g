package stages

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type replayApplierStub struct {
	mu             sync.RWMutex
	pending        bool
	lastApplied    models.OpTime
	lastAppliedSet bool
	batchSizes     []int
	applyErr       error
}

func (a *replayApplierStub) ApplyBatch(_ context.Context, ops []models.Oplog) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batchSizes = append(a.batchSizes, len(ops))
	if a.applyErr != nil {
		return a.applyErr
	}
	a.lastApplied = models.OpTime{TS: ops[len(ops)-1].TS, Term: 7}
	a.lastAppliedSet = true
	return nil
}
func (a *replayApplierStub) Close(context.Context) error { return nil }
func (a *replayApplierStub) HasPendingTransactions() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.pending
}
func (a *replayApplierStub) LastAppliedOpTime() (models.OpTime, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastApplied, a.lastAppliedSet
}
func (a *replayApplierStub) setPending(pending bool) {
	a.mu.Lock()
	a.pending = pending
	a.mu.Unlock()
}

func TestCheckpointingApplierMakesHandledTimestampDurable(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	fsynced := make(chan struct{})
	db.On("Fsync", mock.Anything).Run(func(mock.Arguments) { close(fsynced) }).Return(nil).Once()

	since := models.Timestamp{TS: 100, Inc: 1}
	applied := models.Timestamp{TS: 101, Inc: 2}
	progress := NewReplayProgress(since)
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, 10*time.Millisecond, progress,
		DefaultReplayApplyBatchSize, false)
	ops := make(chan *models.Oplog)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	ops <- &models.Oplog{TS: applied}

	select {
	case <-fsynced:
	case <-time.After(time.Second):
		t.Fatal("fsync was not called")
	}
	close(ops)
	require.NoError(t, <-errC)

	checkpoint := progress.Snapshot()
	require.Equal(t, models.OpTime{TS: applied, Term: 7}, checkpoint.OpTime)
	require.Equal(t, uint64(1), checkpoint.Generation)
}

func TestCheckpointingApplierFinalizesCatchUpCheckpoint(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	finalized := make(chan struct{})
	applied := models.Timestamp{TS: 101, Inc: 2}
	opTime := models.OpTime{TS: applied, Term: 7}
	db.On("ChangeOplogLastTimestamp", mock.Anything, opTime).
		Run(func(mock.Arguments) { close(finalized) }).Return(nil).Once()

	progress := NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, 10*time.Millisecond, progress,
		DefaultReplayApplyBatchSize, true)
	ops := make(chan *models.Oplog)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	ops <- &models.Oplog{TS: applied}

	select {
	case <-finalized:
	case <-time.After(time.Second):
		t.Fatal("catch-up checkpoint was not finalized")
	}
	close(ops)
	require.NoError(t, <-errC)

	checkpoint := progress.Snapshot()
	require.Equal(t, opTime, checkpoint.OpTime)
	require.Equal(t, uint64(1), checkpoint.Generation)
}

func TestCheckpointingApplierDoesNotAdvanceFailedCatchUpCheckpoint(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	applied := models.Timestamp{TS: 101, Inc: 2}
	opTime := models.OpTime{TS: applied, Term: 7}
	db.On("ChangeOplogLastTimestamp", mock.Anything, opTime).
		Return(errors.New("finalization failed")).Once()

	since := models.Timestamp{TS: 100, Inc: 1}
	progress := NewReplayProgress(since)
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, 10*time.Millisecond, progress,
		DefaultReplayApplyBatchSize, true)
	ops := make(chan *models.Oplog)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	ops <- &models.Oplog{TS: applied}

	require.ErrorContains(t, <-errC, "finalization failed")
	checkpoint := progress.Snapshot()
	require.Equal(t, models.OpTime{TS: since}, checkpoint.OpTime)
	require.Zero(t, checkpoint.Generation)
}

func TestCheckpointingApplierFinalizesCatchUpOnStreamEnd(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	applied := models.Timestamp{TS: 101, Inc: 2}
	opTime := models.OpTime{TS: applied, Term: 7}
	db.On("ChangeOplogLastTimestamp", mock.Anything, opTime).Return(nil).Once()

	progress := NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, time.Hour, progress,
		DefaultReplayApplyBatchSize, true)
	ops := make(chan *models.Oplog, 1)
	ops <- &models.Oplog{TS: applied}
	close(ops)

	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.NoError(t, <-errC)
	require.Equal(t, opTime, progress.Snapshot().OpTime)
}

func TestCheckpointingApplierSkipsEmptyCatchUpReplay(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	progress := NewReplayProgress(models.Timestamp{TS: 100, Inc: 1})
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, time.Hour, progress,
		DefaultReplayApplyBatchSize, true)
	ops := make(chan *models.Oplog)
	close(ops)

	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.NoError(t, <-errC)
	require.Zero(t, progress.Snapshot().Generation)
}

func TestCheckpointingApplierWaitsForTransactionBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := clientmocks.NewMongoDriver(t)
		fsynced := make(chan struct{})
		db.On("Fsync", mock.Anything).Run(func(mock.Arguments) { close(fsynced) }).Return(nil).Once()

		stub := &replayApplierStub{pending: true}
		progress := NewReplayProgress(models.Timestamp{TS: 100})
		applier := NewCheckpointingApplier(db, stub, 10*time.Millisecond, progress,
			DefaultReplayApplyBatchSize, false)
		ops := make(chan *models.Oplog)
		errC, err := applier.Apply(t.Context(), ops)
		require.NoError(t, err)
		ops <- &models.Oplog{TS: models.Timestamp{TS: 101}}
		// Advance virtual time and let both timers finish processing while the
		// transaction is pending. A wall-clock sleep does not establish this.
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()

		applied, ok := stub.LastAppliedOpTime()
		require.True(t, ok)
		require.Equal(t, models.Timestamp{TS: 101}, applied.TS)
		checkpoint := progress.Snapshot()
		require.Equal(t, models.Timestamp{TS: 100}, checkpoint.OpTime.TS)
		require.Zero(t, checkpoint.Generation)
		db.AssertNotCalled(t, "Fsync", mock.Anything)

		stub.setPending(false)
		ops <- &models.Oplog{TS: models.Timestamp{TS: 102}}
		select {
		case <-fsynced:
		case <-time.After(time.Second):
			t.Fatal("fsync was not called after transaction boundary")
		}
		close(ops)
		require.NoError(t, <-errC)
		checkpoint = progress.Snapshot()
		require.Equal(t, models.OpTime{TS: models.Timestamp{TS: 102}, Term: 7}, checkpoint.OpTime)
		require.Equal(t, uint64(1), checkpoint.Generation)
	})
}

func TestCheckpointingApplierBatchesFiftyEntries(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	db.On("Fsync", mock.Anything).Return(nil).Once()
	stub := &replayApplierStub{}
	progress := NewReplayProgress(models.Timestamp{TS: 100})
	applier := NewCheckpointingApplier(db, stub, time.Second, progress,
		DefaultReplayApplyBatchSize, false)
	ops := make(chan *models.Oplog, 51)
	for i := 1; i <= 51; i++ {
		ops <- &models.Oplog{TS: models.Timestamp{TS: 101, Inc: uint32(i)}}
	}
	close(ops)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.NoError(t, <-errC)
	require.Equal(t, []int{50, 1}, stub.batchSizes)
	require.Equal(t, models.Timestamp{TS: 101, Inc: 51}, progress.Snapshot().OpTime.TS)
}

func TestCheckpointingApplierUsesConfiguredBatchSize(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	db.On("Fsync", mock.Anything).Return(nil).Once()
	stub := &replayApplierStub{}
	progress := NewReplayProgress(models.Timestamp{TS: 100})
	applier := NewCheckpointingApplier(db, stub, time.Second, progress, 3, false)
	ops := make(chan *models.Oplog, 8)
	for i := 1; i <= 8; i++ {
		ops <- &models.Oplog{TS: models.Timestamp{TS: 101, Inc: uint32(i)}}
	}
	close(ops)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.NoError(t, <-errC)
	require.Equal(t, []int{3, 3, 2}, stub.batchSizes)
	require.Equal(t, models.Timestamp{TS: 101, Inc: 8}, progress.Snapshot().OpTime.TS)
}

func TestCheckpointingApplierFlushesOnStreamEnd(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	db.On("Fsync", mock.Anything).Return(nil).Once()
	stub := &replayApplierStub{}
	applier := NewCheckpointingApplier(db, stub, time.Hour,
		NewReplayProgress(models.Timestamp{TS: 100}), 50, false)
	ops := make(chan *models.Oplog, 1)
	ops <- &models.Oplog{TS: models.Timestamp{TS: 101}}
	close(ops)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	select {
	case err := <-errC:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("batch was not applied when the stream ended")
	}
	require.Equal(t, []int{1}, stub.batchSizes)
}

func TestCheckpointingApplierDoesNotCheckpointFailedBatch(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	progress := NewReplayProgress(models.Timestamp{TS: 100})
	applier := NewCheckpointingApplier(db, &replayApplierStub{applyErr: errors.New("applyOps failed")},
		time.Second, progress, DefaultReplayApplyBatchSize, false)
	ops := make(chan *models.Oplog, 1)
	ops <- &models.Oplog{TS: models.Timestamp{TS: 101}}
	close(ops)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.ErrorContains(t, <-errC, "applyOps failed")
	require.Equal(t, models.Timestamp{TS: 100}, progress.Snapshot().OpTime.TS)
	require.Zero(t, progress.Snapshot().Generation)
}
