package clickhouse

import (
	"context"
	"os/exec"

	"github.com/wal-g/storages/storage"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) {
}
