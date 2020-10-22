package discovery

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

// ResolveStartingTS fetches last-known folder TS or initiates first run from last-known mongoClient TS
func ResolveStartingTS(ctx context.Context, downloader archive.Downloader, mongoClient client.MongoDriver) (models.Timestamp, error) {
	since, err := downloader.LastKnownArchiveTS()
	if err != nil {
		return models.Timestamp{}, fmt.Errorf("can not fetch last-known storage timestamp: %+v", err)
	}
	zeroTS := models.Timestamp{}
	if since != zeroTS {
		tracelog.InfoLogger.Printf("Newest timestamp at storage folder: %v", since)
		return since, nil
	}

	tracelog.InfoLogger.Printf("Initiating archiving first run")
	im, err := mongoClient.IsMaster(ctx)
	if err != nil {
		return models.Timestamp{}, fmt.Errorf("can not fetch LastWrite.MajorityOpTime: %+v", err)
	}
	return im.LastWrite.MajorityOpTime.TS, nil
}

// BuildCursorFromTS finds point to resume archiving or _restarts_ procedure from newest oplog document
func BuildCursorFromTS(ctx context.Context, since models.Timestamp, uploader archive.Uploader, mongoClient client.MongoDriver) (oplogCursor client.OplogCursor, fromTS models.Timestamp, err error) {
	oplogCursor, err = mongoClient.TailOplogFrom(ctx, since)
	if err != nil {
		return nil, models.Timestamp{}, fmt.Errorf("can not build oplog cursor from ts '%s': %+v", since, err)
	}
	if !oplogCursor.Next(ctx) {
		return nil, models.Timestamp{}, fmt.Errorf("can not fetch first document: %+v", oplogCursor.Err())
	}
	rawDoc := oplogCursor.Data()
	if err := oplogCursor.Push(rawDoc); err != nil {
		return nil, models.Timestamp{}, err
	}
	op, err := models.OplogFromRaw(rawDoc)
	if err != nil {
		return nil, models.Timestamp{}, fmt.Errorf("first oplog record decoding failed: %+v", err)
	}

	if op.TS == since {
		return oplogCursor, since, nil
	}

	// since ts is not exists, report gap and continue with newest timestamp
	gapErr := models.NewError(models.SplitFound, fmt.Sprintf("expected first ts is %v, but %v is given", since, op.TS))
	tracelog.ErrorLogger.PrintError(gapErr)

	tracelog.ErrorLogger.Printf("Reinitializing archiving with newest TS")
	im, err := mongoClient.IsMaster(ctx)
	if err != nil {
		return nil, models.Timestamp{}, fmt.Errorf("can not fetch LastWrite.MajorityOpTime: %+v", err)
	}
	newestTS := im.LastWrite.MajorityOpTime.TS
	if err := uploader.UploadGapArchive(gapErr, since, newestTS); err != nil {
		return nil, models.Timestamp{}, err
	}

	oplogCursor, err = mongoClient.TailOplogFrom(ctx, newestTS)
	if err != nil {
		return nil, models.Timestamp{}, fmt.Errorf("can not build oplog cursor from ts '%s': %+v", newestTS, err)
	}

	return oplogCursor, newestTS, nil
}
