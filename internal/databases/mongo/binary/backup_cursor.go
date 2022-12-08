package binary

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/mongo"
)

type BackupCursor struct {
	*mongo.Cursor

	BackupCursorMeta *BackupCursorMeta

	mongodService         *MongodService
	keepAliveStopFunction func()
	closeBackupCursorWg   sync.WaitGroup
	visited               map[string]*BackupFileMeta
}

func CreateBackupCursor(mongodService *MongodService) (*BackupCursor, error) {
	mongoBackupCursor, err := mongodService.GetBackupCursor()
	if err != nil {
		return nil, err
	}

	backupCursor := &BackupCursor{
		Cursor:        mongoBackupCursor,
		mongodService: mongodService,
		visited:       map[string]*BackupFileMeta{},
	}

	err = backupCursor.loadMetadata()
	if err != nil {
		return nil, err
	}

	return backupCursor, nil
}

func (backupCursor *BackupCursor) StartKeepAlive() {
	backupContext, stopPingBackupCursor := context.WithCancel(backupCursor.mongodService.Context)
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
				hasNext := backupCursor.TryNext(backupContext)
				tracelog.InfoLogger.Printf("ping the backup cursor (has next = %v", hasNext)
			}
		}
	}()
}

func (backupCursor *BackupCursor) LoadBackupCursorFiles() (backupFiles []*BackupFileMeta, err error) {
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

	return backupFiles, nil
}

func (backupCursor *BackupCursor) LoadExtendedBackupCursorFiles() (backupFiles []*BackupFileMeta, err error) {
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
	if backupCursor.keepAliveStopFunction != nil { // If we start keep live
		backupCursor.keepAliveStopFunction()
	}
	backupCursor.closeBackupCursorWg.Wait()
	closeErr := backupCursor.Cursor.Close(backupCursor.mongodService.Context)
	if closeErr != nil {
		tracelog.ErrorLogger.Printf("Unable to close backup cursor: %+v", closeErr)
	}
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

func (backupCursor *BackupCursor) createBackupFileMeta(backupFile *BackupCursorFile) (*BackupFileMeta, error) {
	backupFilePath := backupFile.FileName

	if previousBackupFileMeta, ok := backupCursor.visited[backupFilePath]; ok {
		return nil, fmt.Errorf("duplicate backup file <%v> (previous size: %d, new size: %d)",
			backupFile.FileName, previousBackupFileMeta.FileSize, backupFile.FileSize)
	}

	backupFileInfo, err := os.Stat(backupFilePath)
	if err != nil {
		return nil, err
	}

	backupFileMeta := &BackupFileMeta{
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
