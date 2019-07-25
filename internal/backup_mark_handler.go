package internal

import (
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupMark(uploader *Uploader, backupName string, toPermanent bool) {
	baseBackupFolder := uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	uploader.UploadingFolder = baseBackupFolder
	MarkBackup(uploader, baseBackupFolder, backupName, toPermanent)
}
