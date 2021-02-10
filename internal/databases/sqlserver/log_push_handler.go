package sqlserver

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
)

func HandleLogPush(dbnames []string, compression bool) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection()
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, err = getDatabasesToBackup(db, dbnames)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.ErrorLogger.FatalfOnError("failed to list databases to backup: %v", err)

	bs, err := blob.NewServer(folder)
	tracelog.ErrorLogger.FatalfOnError("proxy create error: %v", err)

	err = bs.RunBackground(ctx, cancel)
	tracelog.ErrorLogger.FatalfOnError("proxy run error: %v", err)

	logBackupName := generateLogBackupName()
	baseUrl := getLogBackupUrl(logBackupName)
	err = runParallel(func(dbname string) error {
		return backupSingleItem(LogBackupItem, ctx, db, baseUrl, dbname, compression)
	}, dbnames)
	tracelog.ErrorLogger.FatalfOnError("overall log backup failed: %v", err)

	tracelog.InfoLogger.Printf("log backup finished")
}
