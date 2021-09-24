package sqlserver

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleDatabaseList(backupName string) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("can't find backup %s: %v", backupName, err)
	}
	sentinel := new(SentinelDto)
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalOnError(err)
	for _, name := range sentinel.Databases {
		fmt.Println(name)
	}
}
