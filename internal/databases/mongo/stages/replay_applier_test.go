package stages

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type replayApplierStub struct {
	mu      sync.RWMutex
	pending bool
}

func (a *replayApplierStub) Apply(context.Context, models.Oplog) error { return nil }
func (a *replayApplierStub) Close(context.Context) error               { return nil }
func (a *replayApplierStub) HasPendingTransactions() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.pending
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
	applier := NewCheckpointingApplier(db, &replayApplierStub{}, 10*time.Millisecond, progress)
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

	durable, generation := progress.Snapshot()
	require.Equal(t, applied, durable)
	require.Equal(t, uint64(1), generation)
}

func TestCheckpointingApplierWaitsForTransactionBoundary(t *testing.T) {
	db := clientmocks.NewMongoDriver(t)
	fsynced := make(chan struct{})
	db.On("Fsync", mock.Anything).Run(func(mock.Arguments) { close(fsynced) }).Return(nil).Once()

	stub := &replayApplierStub{pending: true}
	progress := NewReplayProgress(models.Timestamp{TS: 100})
	applier := NewCheckpointingApplier(db, stub, 10*time.Millisecond, progress)
	ops := make(chan *models.Oplog)
	errC, err := applier.Apply(t.Context(), ops)
	require.NoError(t, err)
	ops <- &models.Oplog{TS: models.Timestamp{TS: 101}}
	time.Sleep(30 * time.Millisecond)

	durable, _ := progress.Snapshot()
	require.Equal(t, models.Timestamp{TS: 100}, durable)

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
