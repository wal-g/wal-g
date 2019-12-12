package mongo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleOplogPush(ctx context.Context, uploader *Uploader) {
	lastKnownArchiveTS := fmt.Sprintf("0.0")
	oplogStartTS, err := OplogTimestampFromStr(lastKnownArchiveTS)
	tracelog.ErrorLogger.FatalOnError(err)

	lastKnownTS := oplogStartTS
	batchStartTs := lastKnownTS

	var buf bytes.Buffer // TODO: switch to temp file
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(OplogPath)

	archiveSize, err := internal.GetOplogArchiveAfterSize()
	tracelog.ErrorLogger.FatalOnError(err)

	archiveTimeout, err := internal.GetOplogArchiveTimeout()
	tracelog.ErrorLogger.FatalOnError(err)

	archiveTimer := time.NewTimer(archiveTimeout)
	defer archiveTimer.Stop()

	var wg sync.WaitGroup
	mongodbUrl, ok := internal.GetSetting(internal.MongoDBUriSetting)
	if !ok {
		err := internal.NewUnsetRequiredSettingError(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	oplogFetcher := NewOplogFetcherDB(mongodbUrl, &wg)
	ch, err := oplogFetcher.GetOplogFrom(ctx, lastKnownTS)
	tracelog.ErrorLogger.FatalOnError(err)
	defer wg.Wait()

	for {
		select {
		case op, ok := <-ch:
			// TODO: filter doc
			// TODO: ensure first doc is with lastKnownTS
			// TODO: validate doc: report error if op.NS == admin.system.version or op.OP == renameCollections
			if !ok {
				return
			}
			tracelog.ErrorLogger.FatalOnError(op.Err) // TODO: handle errors

			lastKnownTS = op.TS
			buf.Write(op.Data)
			if buf.Len() < archiveSize {
				continue
			}
			tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

		case <-archiveTimer.C:
			if buf.Len() == 0 {
				continue
			}
			tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
		}

		archiveTimer.Reset(archiveTimeout)
		err := uploader.uploadOplogStream(&buf, batchStartTs, lastKnownTS)
		tracelog.ErrorLogger.FatalOnError(err) // TODO: handle errors

		buf.Reset()
		batchStartTs = lastKnownTS
	}
}
