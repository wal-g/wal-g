package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// MarkBackup marks a backup as permanent or impermanent
func MarkBackup(uploader internal.Uploader, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)
	internal.HandleBackupMark(uploader, backupName, toPermanent, NewGenericMetaInteractor())
}
