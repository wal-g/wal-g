package redis

import (
	"fmt"
	"io"
	"reflect"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupInfo(folder storage.Folder, backupName string, output io.Writer, tag string) {
	backupDetails, err := archive.SentinelWithExistenceCheck(folder, backupName)
	tracelog.ErrorLogger.FatalOnError(err)

	if tag != "" {
		v, err := getField(&backupDetails, tag)
		tracelog.ErrorLogger.FatalOnError(err)
		_, err = fmt.Fprintln(output, v)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	pretty := false
	json := true
	err = printlist.List([]printlist.Entity{backupDetails}, output, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backup info: %v", err)
}

func getField(v *archive.Backup, field string) (string, error) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	if f.IsValid() {
		return f.String(), nil
	}
	return "", fmt.Errorf("no %s field in struct %v", field, &v)
}
