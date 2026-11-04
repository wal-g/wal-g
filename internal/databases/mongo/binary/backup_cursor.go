package binary

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	backupCursorKeepAliveInterval = time.Minute
	backupCursorPingTimeout       = time.Minute
)

type BackupCursor struct {
	*mongo.Cursor

	BackupCursorMeta *BackupCursorMeta

	mongodService         *MongodService
	keepAliveStopFunction func()
	closeBackupCursorWg   sync.WaitGroup
	visited               map[string]*internal.BackupFileMeta
}

func CreateBackupCursor(mongodService *MongodService) (*BackupCursor, error) {
	mongoBackupCursor, err := mongodService.GetBackupCursor()
	if err != nil {
		return nil, errors.Wrap(err, "unable to open backup cursor")
	}

	backupCursor := &BackupCursor{
		Cursor:        mongoBackupCursor,
		mongodService: mongodService,
		visited:       map[string]*internal.BackupFileMeta{},
	}

	err = backupCursor.loadMetadata()
	if err != nil {
		return nil, errors.Wrap(err, "unable to load metadata")
	}

	return backupCursor, nil
}

func (backupCursor *BackupCursor) StartKeepAlive(cancelBackup context.CancelCauseFunc) {
	backupContext, stopPingBackupCursor := context.WithCancel(backupCursor.mongodService.Context)
	backupCursor.keepAliveStopFunction = stopPingBackupCursor
	backupCursor.closeBackupCursorWg.Add(1)
	go func() {
		defer backupCursor.closeBackupCursorWg.Done()
		defer tracelog.InfoLogger.Printf("stop process with ping the backup cursor")
		ticker := time.NewTicker(backupCursorKeepAliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-backupContext.Done():
				return
			case <-ticker.C:
				if backupContext.Err() != nil {
					return
				}
				err := backupCursor.ping(backupCursor.mongodService.Context)
				if backupContext.Err() != nil {
					return
				}
				if err != nil {
					err = errors.Wrap(err, "backup cursor keepalive failed")
					tracelog.ErrorLogger.Printf("%+v", err)
					cancelBackup(err)
					return
				}
			}
		}
	}()
}

func (backupCursor *BackupCursor) LoadBackupCursorFiles() (backupFiles []*internal.BackupFileMeta, err error) {
	for backupCursor.TryNext(backupCursor.mongodService.Context) {
		var backupFile BackupCursorFile
		err = backupCursor.Decode(&backupFile)
		if err != nil {
			return nil, err
		}

		backupFileMeta, err := backupCursor.createBackupFileMeta(&backupFile)
		if err != nil {
			return nil, err
		}

		backupFiles = append(backupFiles, backupFileMeta)
	}
	if err := backupCursor.cursorStateError(); err != nil {
		return nil, errors.Wrap(err, "backup cursor failed while listing files")
	}

	return backupFiles, nil
}

func (backupCursor *BackupCursor) LoadExtendedBackupCursorFiles() (backupFiles []*internal.BackupFileMeta, err error) {
	extendedBackupCursor, err := backupCursor.mongodService.GetBackupCursorExtended(backupCursor.BackupCursorMeta)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to take extended backup cursor with '%+v'", backupCursor.BackupCursorMeta)
	}
	defer func() {
		closeErr := extendedBackupCursor.Close(backupCursor.mongodService.Context)
		if closeErr != nil {
			tracelog.ErrorLogger.Printf("Unable to close backup cursor extended: %+v", closeErr)
		}
	}()

	var journalFiles []BackupCursorFile
	err = extendedBackupCursor.All(backupCursor.mongodService.Context, &journalFiles)
	if err != nil {
		return nil, err
	}

	for _, journalFile := range journalFiles {
		backupFileMeta, err := backupCursor.createBackupFileMeta(&journalFile)
		if err != nil {
			return nil, err
		}

		backupFiles = append(backupFiles, backupFileMeta)
	}

	return backupFiles, nil
}

func (backupCursor *BackupCursor) Close() {
	backupCursor.stopKeepAlive()
	closeErr := backupCursor.Cursor.Close(backupCursor.mongodService.Context)
	if closeErr != nil {
		tracelog.ErrorLogger.Printf("Unable to close backup cursor: %+v", closeErr)
	}
}

func (backupCursor *BackupCursor) StopKeepAliveAndValidate(backupContext context.Context) error {
	backupCursor.stopKeepAlive()
	if err := context.Cause(backupContext); err != nil {
		return err
	}
	if err := backupCursor.ping(backupCursor.mongodService.Context); err != nil {
		return errors.Wrap(err, "backup cursor final check failed")
	}
	return nil
}

func (backupCursor *BackupCursor) ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	pingContext, cancel := context.WithTimeout(ctx, backupCursorPingTimeout)
	defer cancel()
	hasNext := backupCursor.TryNext(pingContext)
	tracelog.InfoLogger.Printf("ping the backup cursor (has next = %v)", hasNext)
	return backupCursor.cursorStateError()
}

func (backupCursor *BackupCursor) stopKeepAlive() {
	if backupCursor.keepAliveStopFunction != nil {
		backupCursor.keepAliveStopFunction()
	}
	backupCursor.closeBackupCursorWg.Wait()
}

func (backupCursor *BackupCursor) cursorStateError() error {
	if err := backupCursor.Err(); err != nil {
		return err
	}
	if backupCursor.ID() == 0 {
		return errors.New("backup cursor is closed")
	}
	return nil
}

func (backupCursor *BackupCursor) loadMetadata() error {
	if exists := backupCursor.Next(backupCursor.mongodService.Context); !exists {
		return fmt.Errorf("invalid state: backup cursor don't have metadata")
	}
	metadataHolder := struct {
		Metadata BackupCursorMeta `bson:"metadata"`
	}{}
	err := backupCursor.Decode(&metadataHolder)
	if err != nil {
		return errors.Wrap(err, "unable to decode metadata")
	}
	tracelog.DebugLogger.Printf("backup cursor metadata: %v", metadataHolder)

	if metadataHolder.Metadata.DBPath == "" {
		return fmt.Errorf("invalid state: empty metadata")
	}
	backupCursor.BackupCursorMeta = &metadataHolder.Metadata

	return nil
}

func (backupCursor *BackupCursor) createBackupFileMeta(backupFile *BackupCursorFile) (*internal.BackupFileMeta, error) {
	backupFilePath := backupFile.FileName

	if previousBackupFileMeta, ok := backupCursor.visited[backupFilePath]; ok {
		return nil, fmt.Errorf("duplicate backup file <%v> (previous size: %d, new size: %d)",
			backupFile.FileName, previousBackupFileMeta.FileSize, backupFile.FileSize)
	}

	backupFileInfo, err := os.Stat(backupFilePath)
	if err != nil {
		return nil, err
	}

	backupFileMeta := &internal.BackupFileMeta{
		Path:     backupFilePath,
		FileMode: backupFileInfo.Mode(),
		FileSize: backupFile.FileSize,
	}
	if backupFileMeta.FileSize <= 0 { // backupFile.FileSize contains 0 for extended backup cursor
		backupFileMeta.FileSize = backupFileInfo.Size()
	}

	backupCursor.visited[backupFilePath] = backupFileMeta

	return backupFileMeta, nil
}
