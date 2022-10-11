package binary

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BackupService struct {
	Context       context.Context
	MongodService *MongodService
	LocalStorage  *LocalStorage
	BackupStorage *BackupStorage

	Sentinel                  models.Backup
	MongodBackupFilesMetadata MongodBackupFilesMetadata

	Visited map[string]*BackupFileMeta
}

func GenerateNewBackupName() string {
	return common.BinaryBackupType + "_" + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func CreateBackupService(ctx context.Context, mongodService *MongodService, localStorage *LocalStorage,
	backupStorage *BackupStorage) (*BackupService, error) {
	return &BackupService{
		Context:       ctx,
		MongodService: mongodService,
		LocalStorage:  localStorage,
		BackupStorage: backupStorage,

		Visited: map[string]*BackupFileMeta{},
	}, nil
}

func (backupService *BackupService) DoBackup(backupName string, permanent bool) (err error) {
	err = backupService.InitializeMongodBackupMeta(backupName, permanent)
	if err != nil {
		return err
	}

	backupCursor, err := backupService.MongodService.GetBackupCursor()
	if err != nil {
		return err
	}
	defer backupCursor.Close()

	backupID, err := backupService.processBackupCursor(backupCursor)
	if err != nil {
		return errors.Wrapf(err, "unable to process backup cursor")
	}
	backupCursor.StartKeepAlive()

	upto := backupService.Sentinel.MongoMeta.BackupLastTS
	extendedBackupCursor, err := backupService.MongodService.GetBackupCursorExtended(backupID, upto)
	if err != nil {
		return errors.Wrapf(err, "unable to take extended backup cursor with '%+v' and '%+v'", backupID, upto)
	}
	defer func() { _ = extendedBackupCursor.Close(backupService.Context) }()

	var journalFiles []BackupCursorFile
	err = extendedBackupCursor.All(backupService.Context, &journalFiles)
	if err != nil {
		return err
	}
	for _, journalFile := range journalFiles {
		err = backupService.AppendBackupFile(journalFile)
		if err != nil {
			return err
		}
	}

	uncompressedSize, compressedSize, err := backupService.UploadBackupFiles()
	if err != nil {
		return err
	}

	return backupService.FinalizeAndStoreMongodBackupMetadata(uncompressedSize, compressedSize)
}

func (backupService *BackupService) processBackupCursor(backupCursor *BackupCursor) (*primitive.Binary, error) {
	var backupCursorMeta *BackupCursorMeta

	for backupCursor.cursor.TryNext(backupService.Context) {
		// metadata is the first record in backup cursor
		if backupCursorMeta == nil {
			metadataHolder := struct {
				Metadata BackupCursorMeta `bson:"metadata"`
			}{}
			err := backupCursor.cursor.Decode(&metadataHolder)
			if err != nil {
				return nil, errors.Wrap(err, "unable to decode metadata")
			}
			tracelog.DebugLogger.Printf("backup cursor metadata: %v", metadataHolder)

			backupCursorMeta = &metadataHolder.Metadata

			err = backupService.processBackupMetadata(backupCursorMeta)
			if err != nil {
				return nil, err
			}
		} else {
			var backupFile BackupCursorFile
			err := backupCursor.cursor.Decode(&backupFile)
			if err != nil {
				return nil, err
			}

			err = backupService.AppendBackupFile(backupFile)
			if err != nil {
				return nil, err
			}
		}
	}

	return &backupCursorMeta.ID, nil
}

func (backupService *BackupService) processBackupMetadata(backupCursorMeta *BackupCursorMeta) error {
	// just to check that we don't have bugs
	if backupService.LocalStorage.MongodDBPath != backupCursorMeta.DBPath {
		return fmt.Errorf("inconsistency! mongod dbPath '%v' != backupCursor dbPath '%v'",
			backupService.LocalStorage.MongodDBPath, backupCursorMeta.DBPath)
	}

	backupService.Sentinel.MongoMeta.BackupLastTS = backupCursorMeta.OplogEnd.TS

	return nil
}

func (backupService *BackupService) InitializeMongodBackupMeta(backupName string, permanent bool) error {
	mongodVersion, err := backupService.MongodService.MongodVersion()
	if err != nil {
		return err
	}

	userData, err := internal.GetSentinelUserData()
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	backupService.Sentinel.BackupName = backupName
	backupService.Sentinel.BackupType = common.BinaryBackupType
	backupService.Sentinel.Hostname = hostname
	backupService.Sentinel.MongoMeta.Version = mongodVersion
	backupService.Sentinel.StartLocalTime = utility.TimeNowCrossPlatformLocal()
	backupService.Sentinel.Permanent = permanent
	backupService.Sentinel.UserData = userData

	return nil
}

func (backupService *BackupService) AppendBackupFile(backupFile BackupCursorFile) error {
	absoluteBackupFilePath := backupFile.FileName
	backupFilePath, err := backupService.LocalStorage.GetRelativeMongodPath(absoluteBackupFilePath)
	if err != nil {
		return err
	}

	if previousBackupFileMeta, ok := backupService.Visited[backupFilePath]; ok {
		return processDoubleEncounterTheSameBackupFile(previousBackupFileMeta, backupFile)
	}

	metadata := &backupService.MongodBackupFilesMetadata
	directoryPath := filepath.Dir(backupFilePath)

	for len(directoryPath) > 0 && directoryPath != "." {
		if _, ok := backupService.Visited[directoryPath]; ok {
			break
		}

		directoryStat, err := os.Stat(backupService.LocalStorage.GetAbsolutePath(directoryPath))
		if err != nil {
			return err
		}

		backupFileMeta := &BackupFileMeta{
			Path:     directoryPath,
			FileMode: directoryStat.Mode(),
		}
		metadata.BackupFiles = append(metadata.BackupFiles, backupFileMeta)
		backupService.Visited[directoryPath] = backupFileMeta

		directoryPath = filepath.Dir(directoryPath)
	}

	backupFileInfo, err := os.Stat(absoluteBackupFilePath)
	if err != nil {
		return err
	}

	backupFileMeta := &BackupFileMeta{
		Path:     backupFilePath,
		FileMode: backupFileInfo.Mode(),
		FileSize: backupFile.FileSize,
	}
	if backupFileMeta.FileSize <= 0 { // backupFile.FileSize contains 0 for extended backup cursor
		backupFileMeta.FileSize = backupFileInfo.Size()
	}
	metadata.BackupFiles = append(metadata.BackupFiles, backupFileMeta)
	backupService.Visited[backupFilePath] = backupFileMeta

	return nil
}

func processDoubleEncounterTheSameBackupFile(backupFileMeta *BackupFileMeta, backupFile BackupCursorFile) error {
	if backupFileMeta.FileSize > backupFile.FileSize {
		return fmt.Errorf("previous backup file <%v> was bigger (was truncate?)", backupFile.FileName)
	}
	if backupFileMeta.FileSize == backupFile.FileSize {
		return fmt.Errorf("previous backup file <%v> has the same file size (bug with uniqueness?)",
			backupFile.FileName)
	}

	tracelog.WarningLogger.Printf("backup file <%v> was processed already with size %v, but new file has size %v",
		backupFileMeta.Path, backupFileMeta.FileSize, backupFile.FileSize)
	backupFileMeta.FileSize = backupFile.FileSize
	return nil
}

func (backupService *BackupService) FinalizeAndStoreMongodBackupMetadata(uncompressedSize, compressedSize int64) error {
	err := backupService.BackupStorage.UploadMongodBackupFilesMetadata(&backupService.MongodBackupFilesMetadata)
	if err != nil {
		return errors.Wrap(err, "can not upload files metadata")
	}

	sentinel := &backupService.Sentinel
	sentinel.FinishLocalTime = utility.TimeNowCrossPlatformLocal()
	sentinel.UncompressedSize = uncompressedSize
	sentinel.CompressedSize = compressedSize

	backupLastTS := models.TimestampFromBson(sentinel.MongoMeta.BackupLastTS)
	sentinel.MongoMeta.Before.LastMajTS = backupLastTS
	sentinel.MongoMeta.Before.LastTS = backupLastTS
	sentinel.MongoMeta.After.LastMajTS = backupLastTS
	sentinel.MongoMeta.After.LastTS = backupLastTS

	return backupService.BackupStorage.UploadSentinel(sentinel)
}

func (backupService *BackupService) UploadBackupFiles() (int64, int64, error) {
	uploader := backupService.BackupStorage.Uploader
	backupName := backupService.BackupStorage.BackupName
	mongodDBPath := backupService.LocalStorage.MongodDBPath

	concurrentUploader, err := CreateConcurrentUploader(uploader, backupName, mongodDBPath)
	if err != nil {
		return 0, 0, err
	}

	for _, backupFileMeta := range backupService.MongodBackupFilesMetadata.BackupFiles {
		backupFilePath := backupService.LocalStorage.GetAbsolutePath(backupFileMeta.Path)

		err = concurrentUploader.Upload(backupFilePath, backupFileMeta)
		if err != nil {
			return 0, 0, err
		}
	}

	return concurrentUploader.Finalize()
}
