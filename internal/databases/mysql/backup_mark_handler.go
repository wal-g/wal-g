package mysql

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// MarkBackup marks a backup as permanent or impermanent
func MarkBackup(ctx context.Context, uploader internal.Uploader, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)
	internal.HandleBackupMark(ctx, uploader, backupName, toPermanent, NewGenericMetaInteractor())
}
