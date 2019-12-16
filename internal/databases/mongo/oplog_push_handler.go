package mongo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"
)

// HandleOplogPush starts oplog archiving process: fetch, validate, upload to storage.
// TODO: unit tests
func HandleOplogPush(ctx context.Context, oplogFetcher oplog.Fetcher, uploader *Uploader, validator oplog.Validator) {
	oplogFolder := uploader.UploadingFolder.GetSubFolder(OplogPath)
	uploader.UploadingFolder = oplogFolder

	checkFirstTS := true
	lastKnownTS, initial, err := DiscoveryArchiveResumeTS(oplogFolder)
	if initial {
		checkFirstTS = false
		tracelog.InfoLogger.Printf("Initiating archiving first run")
		// TODO: register archiving initiation
	}
	batchStartTs := lastKnownTS

	var buf bytes.Buffer // TODO: switch to temp file
	archiveSize, err := internal.GetOplogArchiveAfterSize()
	tracelog.ErrorLogger.FatalOnError(err)

	archiveTimeout, err := internal.GetOplogArchiveTimeout()
	tracelog.ErrorLogger.FatalOnError(err)

	archiveTimer := time.NewTimer(archiveTimeout)
	defer archiveTimer.Stop()

	var wg sync.WaitGroup
	tracelog.InfoLogger.Printf("Starting archiving from last known timestamp: %s", lastKnownTS)
	ch, err := oplogFetcher.GetOplogFrom(ctx, lastKnownTS, &wg)
	tracelog.ErrorLogger.FatalOnError(err)
	defer wg.Wait()

	for {
		select {
		case op, ok := <-ch:
			if !ok {
				return
			}
			// TODO: filter oplog records
			tracelog.ErrorLogger.FatalOnError(op.Err) // TODO: handle errors
			if checkFirstTS {
				// TODO: report and handle gap (place marker record in storage)
				if op.TS != lastKnownTS {
					tracelog.ErrorLogger.FatalOnError(oplog.NewError(oplog.SplitFound,
						fmt.Sprintf("storage last ts %v, but database %v", lastKnownTS, op.TS)))
				}
				checkFirstTS = false
			}

			// TODO: refactor validate func to struct - we need to check the first one oplog op
			err := validator.ValidateRecord(op)
			// TODO: handle errors: mark backup broken and continue
			tracelog.ErrorLogger.FatalOnError(err)

			lastKnownTS = op.TS
			buf.Write(op.Data)
			if buf.Len() < archiveSize {
				continue
			}
			tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

		case <-archiveTimer.C:
			if buf.Len() == 0 {
				utility.ResetTimer(archiveTimer, archiveTimeout)
				continue
			}
			tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
		}
		utility.ResetTimer(archiveTimer, archiveTimeout)

		err := uploader.uploadOplogStream(&buf, batchStartTs, lastKnownTS)
		tracelog.ErrorLogger.FatalOnError(err) // TODO: handle errors

		buf.Reset()
		batchStartTs = lastKnownTS
	}
}
