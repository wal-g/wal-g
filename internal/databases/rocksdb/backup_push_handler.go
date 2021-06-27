package rocksdb

import (
	"bytes"
	"os"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal"
)

// Creates backup of Database with dbOptions to Uploader subfolder
func HandleBackupPush(uploader *internal.Uploader, dbOptions DatabaseOptions) error {
	tempDir, err := os.MkdirTemp("", internal.ROCKSDB)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	var backupInfo BackupInfo
	if backupInfo, err = saveBackupToLocalDirectory(tempDir, dbOptions); err != nil {
		return err
	}

	var buffer bytes.Buffer
	if err = packDirectory(tempDir, &buffer); err != nil {
		return err
	}

	backupName, err := uploader.PushStream(&buffer)
	if err != nil {
		return err
	}
	backupInfo.BackupName = backupName

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
