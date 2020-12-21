package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type MarkHandler struct {
	Mark func(uploader *Uploader, folder storage.Folder, backupName string, toPermanent bool)
}

func (m *MarkHandler) HandleBackupMark(uploader *Uploader, backupName string, toPermanent bool) {
	folder := uploader.UploadingFolder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	m.Mark(uploader, folder, backupName, toPermanent)
}
