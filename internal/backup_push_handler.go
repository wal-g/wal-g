package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"

	"github.com/pkg/errors"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
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

func getPreviousBackup(folder storage.Folder) *Backup {
	incrementCount := 1
	maxDeltas, fromFull := getDeltaConfig()
	basebackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	name, err := GetLatestBackupName(folder)
	if err != nil {
		if _, ok := err.(NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return nil
		} else {
			tracelog.ErrorLogger.FatalError(err)
		}
	}
	backup := NewBackup(basebackupFolder, name)
	sentinelDto, err := backup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)
	metadata, err := backup.GetMeta()
	tracelog.ErrorLogger.FatalOnError(err)
	if sentinelDto.IncrementCount != nil {
		incrementCount = *sentinelDto.IncrementCount + 1
	}

	if incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return nil
	}
	if metadata.StartLsn == InvalidLSN {
		tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
		return nil
	}
	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")
		name = *sentinelDto.IncrementFullName
		backup = NewBackup(basebackupFolder, name)
		metadata, err = backup.GetMeta()
		tracelog.ErrorLogger.FatalOnError(err)
	}
	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x. \n", name, metadata.StartLsn)

	return backup
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(uploader *Uploader, archiveDirectory string, isPermanent bool, isFullBackup bool) {
	archiveDirectory = utility.ResolveSymlink(archiveDirectory)
	maxDeltas, _ := getDeltaConfig()

	var previousBackup *Backup

	folder := uploader.UploadingFolder
	basebackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	if maxDeltas > 0 && !isFullBackup {
		previousBackup = getPreviousBackup(folder)
	} else {
		tracelog.InfoLogger.Println("Doing full backup.")
	}

	uploader.UploadingFolder = basebackupFolder // TODO: AB: this subfolder switch look ugly. I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)

	var incrementFromLsn *uint64 = nil
	var previousBackupSentinelDto BackupSentinelDto
	if previousBackup != nil {
		incrementFromLsn = &previousBackup.Metadata.StartLsn
		previousBackupSentinelDto = *previousBackup.SentinelDto
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	tracelog.ErrorLogger.FatalOnError(err)
	backupName, backupStartLSN, pgVersion, dataDir, isReplica, timeline, err := StartBackup(conn,
		utility.CeilTimeUpToMicroseconds(time.Now()).String())
	tracelog.ErrorLogger.FatalOnError(err)
	if previousBackup != nil {
		backupName = backupName + "_D_" + utility.StripWalFileName(previousBackup.Name)
	}

	uploadPooler, err := NewUploadPooler(NewStorageTarBallMaker(backupName, uploader), viper.GetInt64(TarSizeThresholdSetting), ConfigureCrypter())
	tracelog.ErrorLogger.FatalOnError(err)
	bundle := NewBundle(uploadPooler, archiveDirectory, incrementFromLsn, previousBackupSentinelDto.Files, timeline)

	var meta ExtendedMetadataDto
	meta.StartTime = utility.TimeNowCrossPlatformUTC()
	meta.Hostname, _ = os.Hostname()
	meta.IsPermanent = isPermanent

	meta.DataDir = dataDir

	if previousBackup != nil && uploader.getUseWalDelta() {
		err = bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), backupStartLSN)
		if err == nil {
			tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
		} else {
			tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
		}
	}

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(archiveDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)
	err = uploadPooler.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)
	err = bundle.UploadPgControl(uploader.Compressor.FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.UploadLabelFiles(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	timelineChanged := checkTimelineChanged(conn, isReplica, timeline)

	// Wait for all uploads to finish.
	uploader.finish()
	if uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", backupName)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}

	currentBackupSentinelDto := &BackupSentinelDto{
		IncrementFromLSN: incrementFromLsn,
	}
	if previousBackup != nil {
		currentBackupSentinelDto.IncrementFrom = &previousBackup.Name
		if previousBackupSentinelDto.IsIncremental() {
			currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
		} else {
			currentBackupSentinelDto.IncrementFullName = &previousBackup.Name
		}

		currentIncrementCount := 1
		if previousBackupSentinelDto.IncrementCount != nil {
			currentIncrementCount = *previousBackupSentinelDto.IncrementCount + 1
		}
		currentBackupSentinelDto.IncrementCount = &currentIncrementCount
	}

	currentBackupSentinelDto.setFiles(bundle.Files)
	meta.PgVersion = pgVersion
	meta.StartLsn = backupStartLSN
	meta.FinishLsn = finishLsn
	currentBackupSentinelDto.UserData = GetSentinelUserData()

	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if isPermanent && previousBackup != nil {
		MarkBackup(uploader, folder, previousBackup.Name, true)
	}

	err = UploadMetadata(uploader, backupName, meta)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", backupName, err)
		tracelog.ErrorLogger.FatalError(err)
	}
	err = UploadSentinel(uploader, currentBackupSentinelDto, backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
	// logging backup set name
	tracelog.InfoLogger.Println("Wrote backup with name " + backupName)
}

// TODO : unit tests
func UploadMetadata(uploader *Uploader, backupName string, meta ExtendedMetadataDto) error {
	// BackupSentinelDto struct allows nil field for backward compatiobility
	// We do not expect here nil dto since it is new dto to upload
	meta.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()

	metaFile := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return NewSentinelMarshallingError(metaFile, err)
	}

	return uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

// TODO : unit tests
func UploadSentinel(uploader *Uploader, sentinelDto interface{}, backupName string) error {
	sentinelName := backupName + utility.SentinelSuffix

	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return NewSentinelMarshallingError(sentinelName, err)
	}

	return uploader.Upload(sentinelName, bytes.NewReader(dtoBody))
}

// TODO : unit tests
// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, dataDir string, isReplica bool, timeline uint32, err error) {
	var name, lsnStr string
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", 0, queryRunner.Version, "", false, 0, errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, isReplica, dataDir, err = queryRunner.StartBackup(backup)

	if err != nil {
		return "", 0, queryRunner.Version, "", false, 0, err
	}
	lsn, err = pgx.ParseLSN(lsnStr)

	if isReplica {
		name, timeline, err = getWalFilename(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, "", false, 0, err
		}
	} else {
		timeline, err = readTimeline(conn)
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't get current timeline because of error: '%v'\n", err)
		}
	}
	return "base_" + name, lsn, queryRunner.Version, dataDir, isReplica, timeline, nil

}

// TODO : unit tests
// checkTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func checkTimelineChanged(conn *pgx.Conn, isReplica bool, oldTimeline uint32) bool {
	if isReplica {
		timeline, err := readTimeline(conn)
		if err != nil {
			tracelog.ErrorLogger.Printf("Unable to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		// https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
		// Following check is the very pessimistic approach on replica backup invalidation
		if timeline != oldTimeline {
			tracelog.ErrorLogger.Printf("Timeline has changed since backup start. Sentinel for the backup will not be uploaded.")
			return true
		}
	}
	return false
}
