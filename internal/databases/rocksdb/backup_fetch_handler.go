package rocksdb

import (
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(uploader internal.Uploader, dbOptions DatabaseOptions, restoreOptions RestoreOptions) error {
	folder := uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	backup := internal.NewBackup(folder, restoreOptions.BackupName)
	metaData := BackupInfo{}
	if err := backup.FetchSentinel(&metaData); err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", internal.ROCKSDB)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	reader, err := folder.ReadObject(backup.Name)
	if err != nil {
		return err
	}
	if err = unpackStreamToDirectory(tempDir, reader); err != nil {
		return err
	}

	be, err := OpenBackupEngine(tempDir, false)
	if err != nil {
		return err
	}
	defer be.CloseBackupEngine()

	return be.RestoreBackup(dbOptions, metaData.Id)
}
