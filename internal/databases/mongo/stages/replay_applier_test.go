package stages

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"go.mongodb.org/mongo-driver/v2/bson"
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
	time.Sleep(30 * time.Millisecond)

	checkpoint := progress.Snapshot()
	require.Equal(t, models.Timestamp{TS: 100}, checkpoint.OpTime.TS)

	stub.setPending(false)
	ops <- &models.Oplog{TS: models.Timestamp{TS: 102}}
	select {
	case <-fsynced:
	case <-time.After(time.Second):
		t.Fatal("fsync was not called after transaction boundary")
	}
	close(ops)
	require.NoError(t, <-errC)
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

// TestCheckpointingApplierFinalizesCatchUpWithTermOfLastReplayedEntry reproduces
// a catch-up replay whose oplog stream ends with a periodic noop. The DBApplier
// skips noops, yet the noop is still the last entry of the replayed archive
// sequence. The finalized oplog position must keep the term of the replayed
// entries (the primary's term). If the synthetic oplog entry is written with
// term 0, MongoDB reports OplogStartMissing against the primary and the
// replica rolls back forever instead of rejoining the replica set.
func TestCheckpointingApplierFinalizesCatchUpWithTermOfLastReplayedEntry(t *testing.T) {
	mongoDriver := clientmocks.NewMongoDriver(t)
	term := int64(1)
	var finalized *models.OpTime

	mongoDriver.On("ApplyOps", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		ops := args.Get(1).([]*db.Oplog)
		require.Len(t, ops, 1)
		require.Equal(t, "i", ops[0].Operation)
		require.NotNil(t, ops[0].Term)
		require.Equal(t, term, *ops[0].Term)
	}).Return(nil).Once()

	mongoDriver.On("ChangeOplogLastTimestamp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			opTime := args.Get(1).(models.OpTime)
			finalized = &opTime
		}).Return(nil).Once()

	dbApplier := oplog.NewDBApplier(mongoDriver, oplog.DBApplierArgs{PreserveUUID: true, Reconfig: true})
	defer dbApplier.Close(context.Background())

	progress := NewReplayProgress(models.Timestamp{TS: 100})
	applier := NewCheckpointingApplier(mongoDriver, dbApplier, time.Hour, progress,
		DefaultReplayApplyBatchSize, true)

	appliedTS := models.Timestamp{TS: 100, Inc: 1}
	noopTS := models.Timestamp{TS: 100, Inc: 2}
	ops := make(chan *models.Oplog, 2)
	ops <- catchUpReplayEntry(t, appliedTS, "i", term, bson.D{{Key: "_id", Value: int32(1)}})
	ops <- catchUpReplayEntry(t, noopTS, "n", term, bson.D{{Key: "msg", Value: "periodic noop"}})
	close(ops)

	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	require.NoError(t, <-errC)

	expected := models.OpTime{TS: noopTS, Term: term}
	require.NotNil(t, finalized, "catch-up finalization must set the oplog position")
	require.Equal(t, expected, *finalized,
		"finalized position must keep the term of the last replayed archive entry")
	require.Equal(t, expected, progress.Snapshot().OpTime)
}

func catchUpReplayEntry(
	t *testing.T,
	ts models.Timestamp,
	operation string,
	term int64,
	object bson.D,
) *models.Oplog {
	t.Helper()
	entry := db.Oplog{
		Timestamp: ts.ToBsonTS(),
		Term:      &term,
		Version:   2,
		Operation: operation,
		Namespace: "test.items",
		Object:    object,
	}
	if operation == "n" {
		entry.Namespace = ""
	}
	data, err := bson.Marshal(entry)
	require.NoError(t, err)
	return &models.Oplog{TS: ts, Data: data}
}
