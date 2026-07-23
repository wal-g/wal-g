package redis

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupInfo(ctx context.Context, folder storage.Folder, backupName string, output io.Writer, tag string) {
	backupDetails, err := archive.SentinelWithExistenceCheck(ctx, folder, backupName)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(archive.EnrichWithAttachedTS(ctx, folder, &backupDetails))

	if tag != "" {
		v, err := getField(ctx, folder, &backupDetails, tag)
		tracelog.ErrorLogger.FatalOnError(err)
		_, err = fmt.Fprintln(output, v)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	pretty := false
	json := true
	err = printlist.List([]printlist.Entity{&backupDetails}, output, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backup info: %v", err)
}

func getField(ctx context.Context, folder storage.Folder, v *archive.Backup, field string) (string, error) {
	if field == "Slots" {
		return archive.FetchSlotsDataFromStorage(ctx, folder, v)
	}

	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	if f.IsValid() {
		return f.String(), nil
	}
	return "", fmt.Errorf("no %s field in struct %v", field, &v)
}
