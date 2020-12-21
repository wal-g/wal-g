package mysql

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// MarkBackup marks a backup as permanent or impermanent
func markBackup(uploader *internal.Uploader, folder storage.Folder, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)

	backup := internal.NewBackup(folder, backupName)

	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.WarningLogger.Println("failed to get previous meta, creating new one")
		meta = internal.ExtendedMetadataDto{
			IsPermanent: toPermanent,
		}
	} else {
		meta.IsPermanent = toPermanent
	}

	metadataUploadObject, err := internal.GetMetadataUploadObject(backup.Name, meta)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("Failed to mark previous backups: %v", err)
	}

	err = uploader.UploadMultiple([]internal.UploadObject{
		metadataUploadObject,
	})
	tracelog.ErrorLogger.FatalfOnError("Failed to mark previous backups: %v", err)
}


func NewMysqlMarkBackup() *internal.MarkHandler {
	return &internal.MarkHandler{
		Mark: markBackup,
	}
}
