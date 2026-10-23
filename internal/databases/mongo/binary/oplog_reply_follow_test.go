package binary

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type replayFollowDownloader struct {
	archive.Downloader
	arch models.Archive
	raw  []byte
}

func (d *replayFollowDownloader) ListOplogArchivesSegment(
	_ context.Context,
	startAfter *string,
	_ *string,
) ([]models.Archive, error) {
	if startAfter != nil && strings.HasPrefix(*startAfter, models.ArchiveTypeGap+"_") {
		return nil, nil
	}
	return []models.Archive{d.arch}, nil
}

func (d *replayFollowDownloader) DownloadOplogArchive(
	_ context.Context,
	_ models.Archive,
	writer io.WriteCloser,
) error {
	_, err := writer.Write(d.raw)
	return err
}

type replayFollowApplier struct {
	last models.OpTime
}

func (a *replayFollowApplier) ApplyBatch(_ context.Context, ops []models.Oplog) error {
	a.last = models.OpTime{TS: ops[len(ops)-1].TS, Term: 7}
	return nil
}

func (a *replayFollowApplier) Close(context.Context) error  { return nil }
func (a *replayFollowApplier) HasPendingTransactions() bool { return false }
func (a *replayFollowApplier) LastAppliedOpTime() (models.OpTime, bool) {
	return a.last, a.last != (models.OpTime{})
}

func TestHandleFollowingOplogReplayFlushesAndCheckpointsBeforeStopping(t *testing.T) {
	ts := models.Timestamp{TS: 101, Inc: 2}
	raw, err := bson.Marshal(bson.D{{Key: "ts", Value: ts.ToBsonTS()}})
	require.NoError(t, err)
	arch, err := models.NewArchive(models.Timestamp{TS: 100}, ts, "br", models.ArchiveTypeOplog)
	require.NoError(t, err)

	db := clientmocks.NewMongoDriver(t)
	opTime := models.OpTime{TS: ts, Term: 7}
	db.On("ChangeOplogLastTimestamp", mock.Anything, opTime).Return(nil).Once()
	progress := stages.NewReplayProgress(ts)
	fetcher := stages.NewFollowingStorageFetcher(
		&replayFollowDownloader{arch: arch, raw: raw}, time.Millisecond)
	applier := stages.NewCheckpointingApplier(
		db, &replayFollowApplier{}, time.Hour, progress, stages.DefaultReplayApplyBatchSize, true)
	config := FollowLatestConfig{
		PollInterval:      time.Millisecond,
		ObservationWindow: time.Hour,
		CatchUpTimeout:    time.Hour,
		StallTimeout:      time.Hour,
	}
	controller := newCatchUpController(config, progress, fetcher)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err = HandleFollowingOplogReplay(ctx, ts, false, fetcher, applier, controller)

	require.NoError(t, err)
	checkpoint := progress.Snapshot()
	require.Equal(t, opTime, checkpoint.OpTime)
	require.Equal(t, uint64(1), checkpoint.Generation)
}
