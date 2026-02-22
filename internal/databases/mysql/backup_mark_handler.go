package mysql

import (
	"fmt"
	"log/slog"

	"github.com/wal-g/wal-g/internal"
)

// MarkBackup marks a backup as permanent or impermanent
func MarkBackup(uploader internal.Uploader, backupName string, toPermanent bool) {
	slog.Info(fmt.Sprintf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent))
	internal.HandleBackupMark(uploader, backupName, toPermanent, NewGenericMetaInteractor())
}
