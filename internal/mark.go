package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type MarkFn func(uploader *Uploader, folder storage.Folder, backupName string, toPermanent bool)

func HandleBackupMark(f MarkFn, uploader *Uploader, backupName string, toPermanent bool) {
	folder := uploader.UploadingFolder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	f(uploader, folder, backupName, toPermanent)
}
