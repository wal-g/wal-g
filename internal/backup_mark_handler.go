package internal

import (
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupMark(uploader *WalUploader, backupName string, toPermanent bool) {
	folder := uploader.UploadingFolder
	baseBackupFolder := uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	uploader.UploadingFolder = baseBackupFolder
	markBackup(uploader, folder, backupName, toPermanent)
}
