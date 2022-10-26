package binary

import (
	"context"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/mongo"
)

type BackupCursor struct {
	ctx                   context.Context
	cursor                *mongo.Cursor
	keepAliveStopFunction func()
	closeBackupCursorWg   sync.WaitGroup
}

func CreateBackupCursor(ctx context.Context, cursor *mongo.Cursor) *BackupCursor {
	return &BackupCursor{
		ctx:    ctx,
		cursor: cursor,
	}
}

func (backupCursor *BackupCursor) StartKeepAlive() {
	backupContext, stopPingBackupCursor := context.WithCancel(backupCursor.ctx)
	backupCursor.keepAliveStopFunction = stopPingBackupCursor
	backupCursor.closeBackupCursorWg.Add(1)
	go func() {
		defer backupCursor.closeBackupCursorWg.Done()
		ticker := time.NewTicker(time.Minute * 1)
		defer ticker.Stop()
		for {
			select {
			case <-backupContext.Done():
				tracelog.InfoLogger.Printf("stop process with ping the backup cursor")
				return
			case <-ticker.C:
				hasNext := backupCursor.cursor.TryNext(backupContext)
				tracelog.InfoLogger.Printf("ping the backup cursor (has next = %v", hasNext)
			}
		}
	}()
}

func (backupCursor *BackupCursor) Close() {
	if backupCursor.keepAliveStopFunction != nil { // If we start keep live
		backupCursor.keepAliveStopFunction()
	}
	backupCursor.closeBackupCursorWg.Wait()
	closeErr := backupCursor.cursor.Close(backupCursor.ctx)
	if closeErr != nil {
		tracelog.ErrorLogger.Printf("Unable to close backup cursor: %+v", closeErr)
	}
}
