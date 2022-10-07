package binary

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/checksum"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type BackupService struct {
	Context       context.Context
	MongodService *MongodService
	LocalStorage  *LocalStorage
	BackupStorage *BackupStorage

	Sentinel                  models.Backup
	MongodBackupFilesMetadata MongodBackupFilesMetadata

	BackupDirectoriesMap map[string]*BackupDirectoryMeta
	BackupFilesMap       map[string]*BackupFileMeta
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

		BackupDirectoriesMap: map[string]*BackupDirectoryMeta{},
		BackupFilesMap:       map[string]*BackupFileMeta{},
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
	defer func() {
		if err != nil {
			closeErr := backupCursor.Close(backupService.Context)
			if closeErr != nil {
				tracelog.ErrorLogger.Printf("Unable to close backup cursor: %+v", closeErr)
			}
		}
	}()

	backupCursorCloseChannel := make(chan string)
	defer func() {
		backupCursorCloseChannel <- "Game Over"
	}()

	backupID, err := backupService.processBackupCursor(backupCursor, backupCursorCloseChannel)
	if err != nil {
		return errors.Wrapf(err, "unable to process backup cursor")
	}

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

	err = backupService.UploadBackupFiles()
	if err != nil {
		return err
	}

	return backupService.FinalizeAndStoreMongodBackupMetadata()
}

// nolint: whitespace
func (backupService *BackupService) processBackupCursor(backupCursor *mongo.Cursor,
	closeChannel chan string) (*primitive.Binary, error) {

	var backupCursorMeta *BackupCursorMeta

	for backupCursor.TryNext(backupService.Context) {
		// metadata is the first record in backup cursor
		if backupCursorMeta == nil {
			metadataHolder := struct {
				Metadata BackupCursorMeta `bson:"metadata"`
			}{}
			err := backupCursor.Decode(&metadataHolder)
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
			err := backupCursor.Decode(&backupFile)
			if err != nil {
				return nil, err
			}

			err = backupService.AppendBackupFile(backupFile)
			if err != nil {
				return nil, err
			}
		}
	}

	go func() {
		ticker := time.NewTicker(time.Minute * 1)
		defer ticker.Stop()
		for {
			select {
			case <-closeChannel:
				closeErr := backupCursor.Close(context.Background())
				cursorErr := backupCursor.Err()
				fmt.Printf("stop cursor polling: %v, cursor err: %v\n", closeErr, cursorErr)
				return
			case <-ticker.C:
				backupCursor.TryNext(backupService.Context)
			}
		}
	}()

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
	backupFilePath, err := backupService.LocalStorage.GetRelativeMongodPath(backupFile.FileName)
	if err != nil {
		return err
	}

	if previousBackupFileMeta, ok := backupService.BackupFilesMap[backupFilePath]; ok {
		return processDoubleEncounterTheSameBackupFile(previousBackupFileMeta, backupFile)
	}

	metadata := &backupService.MongodBackupFilesMetadata
	directoryPath := filepath.Dir(backupFilePath)

	for len(directoryPath) > 0 && directoryPath != "." {
		if _, ok := backupService.BackupDirectoriesMap[directoryPath]; ok {
			break
		}

		directoryStat, err := os.Stat(backupService.LocalStorage.GetAbsolutePath(directoryPath))
		if err != nil {
			return err
		}

		backupDirectoryMeta := BackupDirectoryMeta{
			Path:     directoryPath,
			FileMode: directoryStat.Mode(),
		}
		metadata.BackupDirectories = append(metadata.BackupDirectories, &backupDirectoryMeta)
		backupService.BackupDirectoriesMap[directoryPath] = &backupDirectoryMeta

		directoryPath = filepath.Dir(directoryPath)
	}

	backupFileStat, err := os.Stat(backupFile.FileName)
	if err != nil {
		return err
	}

	backupFileMeta := BackupFileMeta{
		Path:             backupFilePath,
		FileMode:         backupFileStat.Mode(),
		Compression:      backupService.BackupStorage.GetCompression(),
		UncompressedSize: backupFileStat.Size(), // backupFile.FileSize contains 0 for extended backup cursor
	}
	metadata.BackupFiles = append(metadata.BackupFiles, &backupFileMeta)
	backupService.Sentinel.UncompressedSize += backupFile.FileSize
	backupService.BackupFilesMap[backupFilePath] = &backupFileMeta

	return nil
}

func processDoubleEncounterTheSameBackupFile(backupFileMeta *BackupFileMeta, backupFile BackupCursorFile) error {
	if backupFileMeta.UncompressedSize > backupFile.FileSize {
		return fmt.Errorf("previous backup file <%v> was bigger (was truncate?)", backupFile.FileName)
	}
	if backupFileMeta.UncompressedSize == backupFile.FileSize {
		return fmt.Errorf("previous backup file <%v> has the same file size (bag with uniqueness?)",
			backupFile.FileName)
	}

	tracelog.WarningLogger.Printf("backup file <%v> was processed already with size %v, but new file has size %v",
		backupFileMeta.Path, backupFileMeta.UncompressedSize, backupFile.FileSize)
	backupFileMeta.UncompressedSize = backupFile.FileSize
	return nil
}

func (backupService *BackupService) FinalizeAndStoreMongodBackupMetadata() error {
	err := backupService.BackupStorage.UploadMongodBackupFilesMetadata(&backupService.MongodBackupFilesMetadata)
	if err != nil {
		return errors.Wrap(err, "can not upload files metadata")
	}

	compressedDataSize, err := backupService.BackupStorage.CalculateCompressedFiles(backupService.BackupFilesMap)
	if err != nil {
		return errors.Wrap(err, "unable to calculate compressed files in backup storage")
	}

	sentinel := &backupService.Sentinel
	sentinel.FinishLocalTime = utility.TimeNowCrossPlatformLocal()
	sentinel.CompressedSize = compressedDataSize

	backupLastTS := models.TimestampFromBson(sentinel.MongoMeta.BackupLastTS)
	sentinel.MongoMeta.Before.LastMajTS = backupLastTS
	sentinel.MongoMeta.Before.LastTS = backupLastTS
	sentinel.MongoMeta.After.LastMajTS = backupLastTS
	sentinel.MongoMeta.After.LastTS = backupLastTS

	return backupService.BackupStorage.UploadSentinel(sentinel)
}

func (backupService *BackupService) UploadBackupFiles() error {
	// todo: parallel
	for i := 0; i < len(backupService.MongodBackupFilesMetadata.BackupFiles); i++ {
		backupFileMeta := backupService.MongodBackupFilesMetadata.BackupFiles[i]
		err := backupService.UploadBackupFile(backupFileMeta)
		if err != nil {
			return err
		}
	}
	return nil
}

func (backupService *BackupService) UploadBackupFile(backupFileMeta *BackupFileMeta) error {
	fileReader, err := backupService.LocalStorage.CreateReader(backupFileMeta)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(fileReader, fmt.Sprintf("close backup file reader %v", backupFileMeta.Path))

	var reader = limiters.NewDiskLimitReader(fileReader)

	if backupFileMeta.UncompressedSize >= 0 {
		// backupFileMeta.UncompressedSize contains size from backupCursor
		reader = io.LimitReader(reader, backupFileMeta.UncompressedSize)
	}

	checksumCalculator := checksum.CreateCalculator()
	readerWithChecksum := checksum.CreateReaderWithChecksum(reader, checksumCalculator)

	err = backupService.BackupStorage.UploadFile(readerWithChecksum, backupFileMeta)
	if err != nil {
		return err
	}

	backupFileMeta.Checksum = Checksum{
		Algorithm: checksumCalculator.Algorithm(),
		Data:      checksumCalculator.Checksum(),
	}

	return nil
}
