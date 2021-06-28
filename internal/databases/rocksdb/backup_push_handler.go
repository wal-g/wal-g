package rocksdb

import (
	"bytes"
	"os"

	"github.com/pkg/errors"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

// Creates backup of Database with dbOptions to Uploader subfolder
func HandleBackupPush(uploader *internal.Uploader, dbOptions DatabaseOptions) error {
	tempDir, err := os.MkdirTemp("", internal.ROCKSDB)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Temporaly backup to ", tempDir)
	defer os.RemoveAll(tempDir)

	var backupInfo BackupInfo
	if backupInfo, err = saveBackupToLocalDirectory(tempDir, dbOptions); err != nil {
		return err
	}

	var buffer bytes.Buffer
	if err = packDirectory(tempDir, &buffer); err != nil {
		return err
	}

	backupName := utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)

	if err = uploader.UploadingFolder.PutObject(backupName, &buffer); err != nil {
		return err
	}
	tracelog.InfoLogger.Println("Saved backup with name ", backupName)
	backupInfo.BackupName = backupName

	rawSize, err := uploader.RawDataSize()
	if err != nil {
		tracelog.WarningLogger.PrintError(err)
	}
	backupInfo.RawSize = uint64(rawSize)
	uploadedSize, err := uploader.UploadedDataSize()
	if err != nil {
		tracelog.WarningLogger.PrintError(err)
	}
	backupInfo.BackupSize = uint64(uploadedSize)

	return internal.UploadSentinel(uploader, backupInfo, backupName)
}

func saveBackupToLocalDirectory(backupEnginePath string, dbOptions DatabaseOptions) (BackupInfo, error) {
	be, err := OpenBackupEngine(backupEnginePath, true)
	defaultBackupInfo := BackupInfo{}
	if err != nil {
		return defaultBackupInfo, errors.Wrapf(err, "Error when open backupEngine by path: %s ", backupEnginePath)
	}
	defer be.CloseBackupEngine()

	db, err := OpenDatabase(dbOptions)
	if err != nil {
		return defaultBackupInfo, errors.Wrapf(err, "Error when open database (%s)", dbOptions.DbPath)
	}
	defer db.CloseDb()

	return be.CreateBackup(db)
}
