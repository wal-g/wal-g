package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type SentinelMarshallingError struct {
	error
}

func NewSentinelMarshallingError(sentinelName string, err error) SentinelMarshallingError {
	return SentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err SentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(DeltaMaxStepsSetting)
	if origin, hasOrigin := GetSetting(DeltaOriginSetting); hasOrigin {
		switch origin {
		case LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", DeltaOriginSetting, origin)
		}
	}
	return
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(uploader *Uploader, archiveDirectory string) {
	archiveDirectory = utility.ResolveSymlink(archiveDirectory)
	maxDeltas, fromFull := getDeltaConfig()

	var previousBackupSentinelDto BackupSentinelDto
	var previousBackupName string
	var err error
	incrementCount := 1

	folder := uploader.UploadingFolder
	basebackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	if maxDeltas > 0 {
		previousBackupName, err = GetLatestBackupName(folder)
		if err != nil {
			if _, ok := err.(NoBackupsFoundError); ok {
				tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			} else {
				tracelog.ErrorLogger.FatalError(err)
			}
		} else {
			previousBackup := NewBackup(basebackupFolder, previousBackupName)
			previousBackupSentinelDto, err = previousBackup.FetchSentinel()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			if previousBackupSentinelDto.IncrementCount != nil {
				incrementCount = *previousBackupSentinelDto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
				previousBackupSentinelDto = BackupSentinelDto{}
			} else if previousBackupSentinelDto.BackupStartLSN == nil {
				tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			} else {
				if fromFull {
					tracelog.InfoLogger.Println("Delta will be made from full backup.")
					previousBackupName = *previousBackupSentinelDto.IncrementFullName
					previousBackup := NewBackup(basebackupFolder, previousBackupName)
					previousBackupSentinelDto, err = previousBackup.FetchSentinel()
					if err != nil {
						tracelog.ErrorLogger.FatalError(err)
					}
				}
				tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x. \n", previousBackupName, *previousBackupSentinelDto.BackupStartLSN)
			}
		}
	} else {
		tracelog.InfoLogger.Println("Doing full backup.")
	}

	uploader.UploadingFolder = basebackupFolder // TODO: AB: this subfolder switch look ugly. I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)

	crypter := ConfigureCrypter()
	bundle := NewBundle(archiveDirectory, crypter, previousBackupSentinelDto.BackupStartLSN, previousBackupSentinelDto.Files)

	var meta ExtendedMetadataDto
	meta.StartTime = utility.TimeNowCrossPlatformUTC()
	meta.Hostname, _ = os.Hostname()
	meta.IsPermanent = false // TODO: add permanent command flag

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	backupName, backupStartLSN, pgVersion, dataDir, err := bundle.StartBackup(conn,
		utility.CeilTimeUpToMicroseconds(time.Now()).String())
	meta.DataDir = dataDir
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	if len(previousBackupName) > 0 && previousBackupSentinelDto.BackupStartLSN != nil {
		if uploader.getUseWalDelta() {
			err = bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), backupStartLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
			}
		}
		backupName = backupName + "_D_" + utility.StripWalFileName(previousBackupName)
	}

	bundle.TarBallMaker = NewStorageTarBallMaker(backupName, uploader)

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	bundle.StartQueue()
	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(archiveDirectory, bundle.HandleWalkedFSObject)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	err = bundle.FinishQueue()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	err = bundle.UploadPgControl(uploader.Compressor.FileExtension())
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.UploadLabelFiles(conn)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	timelineChanged := bundle.checkTimelineChanged(conn)

	// Wait for all uploads to finish.
	uploader.finish()
	if uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", backupName)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}

	currentBackupSentinelDto := &BackupSentinelDto{
		BackupStartLSN:   &backupStartLSN,
		IncrementFromLSN: previousBackupSentinelDto.BackupStartLSN,
		PgVersion:        pgVersion,
	}
	if previousBackupSentinelDto.BackupStartLSN != nil {
		currentBackupSentinelDto.IncrementFrom = &previousBackupName
		if previousBackupSentinelDto.IsIncremental() {
			currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
		} else {
			currentBackupSentinelDto.IncrementFullName = &previousBackupName
		}
		currentBackupSentinelDto.IncrementCount = &incrementCount
	}

	currentBackupSentinelDto.setFiles(bundle.GetFiles())
	currentBackupSentinelDto.BackupFinishLSN = &finishLsn
	currentBackupSentinelDto.UserData = GetSentinelUserData()

	err = UploadMetadata(uploader, currentBackupSentinelDto, backupName, meta)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", backupName, err)
	}
	// If other parts are successful in uploading, upload json file.
	err = UploadSentinel(uploader, currentBackupSentinelDto, backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	// logging backup set name
	tracelog.InfoLogger.Println("Wrote backup with name " + backupName)
}

// TODO : unit tests
func UploadMetadata(uploader *Uploader, sentinelDto *BackupSentinelDto, backupName string, meta ExtendedMetadataDto) error {
	// BackupSentinelDto struct allows nil field for backward compatiobility
	// We do not expect here nil dto since it is new dto to upload
	meta.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()
	meta.StartLsn = *sentinelDto.BackupStartLSN
	meta.FinishLsn = *sentinelDto.BackupFinishLSN
	meta.PgVersion = sentinelDto.PgVersion

	metaFile := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return NewSentinelMarshallingError(metaFile, err)
	}

	return uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

// TODO : unit tests
func UploadSentinel(uploader *Uploader, sentinelDto *BackupSentinelDto, backupName string) error {
	sentinelName := backupName + utility.SentinelSuffix

	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return NewSentinelMarshallingError(sentinelName, err)
	}

	return uploader.Upload(sentinelName, bytes.NewReader(dtoBody))
}
