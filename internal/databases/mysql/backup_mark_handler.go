package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

// MarkBackup marks a backup as permanent or impermanent
func MarkBackup(uploader *internal.Uploader, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)

	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	backup := internal.NewBackup(uploader.UploadingFolder, backupName)

	if exists, err := backup.CheckExistence(); err != nil {
		tracelog.ErrorLogger.Fatalf("failed to check backup exstance %v", err)
	} else if !exists {
		tracelog.ErrorLogger.Fatalf("desired backup does not exist %s", backup.Name)
	}

	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.WarningLogger.Println("failed to get previous meta, creating new one")
		meta = internal.ExtendedMetadataDto{
			IsPermanent: toPermanent,
		}
	} else {
		meta.IsPermanent = toPermanent
	}

	metadataUploadObject, err := internal.GetMetadataUploadObject(backup.Name, &meta)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("Failed to mark previous backups: %v", err)
	}

	err = uploader.UploadMultiple([]internal.UploadObject{
		metadataUploadObject,
	})
	tracelog.ErrorLogger.FatalfOnError("Failed to mark previous backups: %v", err)
}
