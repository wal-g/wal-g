package mongo

import (
	"io"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/internal/databases/mongo/logical"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// HandleBackupShow prints sentinel contents.
func HandleBackupShow(backupFolder storage.Folder, backupName string, output io.Writer, pretty bool) (err error) {
	sentinel, err := DownloadSentinel(backupFolder, backupName)
	if err != nil {
		return err
	}

	return internal.WriteAsJSON(sentinel, output, pretty)
}

func DownloadSentinel(folder storage.Folder, backupName string) (interface{}, error) {
	if strings.HasPrefix(backupName, "binary") {
		return binary.DownloadSentinel(folder, backupName)
	}
	return logical.DownloadSentinel(folder, backupName)
}
