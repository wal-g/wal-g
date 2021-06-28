package rocksdb

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(uploader *internal.Uploader, dbOptions DatabaseOptions, restoreOptions RestoreOptions) error {
	folder := uploader.UploadingFolder

	backup, err := internal.GetBackupByName(restoreOptions.BackupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	metaData := BackupInfo{}
	if err := backup.FetchSentinel(&metaData); err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", internal.ROCKSDB)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Temp directory to store backup is ", tempDir)
	defer os.RemoveAll(tempDir)

	folder = folder.GetSubFolder(utility.BaseBackupPath)
	reader, err := folder.ReadObject(backup.Name)
	if err != nil {
		return err
	}
	if err = unpackStreamToDirectory(tempDir, reader); err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Downloaded backup to temp directory")

	be, err := OpenBackupEngine(tempDir, false)
	if err != nil {
		return err
	}
	defer be.CloseBackupEngine()

	tracelog.DebugLogger.Printf("Restoring backup %s: backupId=%d\n", backup.Name, metaData.Id)
	return be.RestoreBackup(dbOptions, metaData.Id)
}
