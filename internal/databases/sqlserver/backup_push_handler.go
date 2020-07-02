package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
	"net/url"
	"os"
	"syscall"
)

func HandleBackupPush(dbnames []string) {
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

	server, _ := os.Hostname()
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName := generateBackupName()
	baseUrl := getBackupUrl(backupName)

	err = runParallel(func(dbname string) error {
		return backupSingleDatabase(ctx, db, baseUrl, dbname)
	}, dbnames)
	tracelog.ErrorLogger.FatalfOnError("overall backup failed: %v", err)

	sentinel := &SentinelDto{
		Server:         server,
		Databases:      dbnames,
		StartLocalTime: timeStart,
	}
	uploader := internal.NewUploader(nil, folder.GetSubFolder(utility.BaseBackupPath))
	tracelog.InfoLogger.Printf("uploading sentinel: %s", sentinel)
	err = internal.UploadSentinel(uploader, sentinel, backupName)
	tracelog.ErrorLogger.FatalfOnError("failed to save sentinel: %v", err)

	tracelog.InfoLogger.Printf("backup finished")
}

func backupSingleDatabase(ctx context.Context, db *sql.DB, baseUrl string, dbname string) error {
	backupUrl := fmt.Sprintf("%s/%s", baseUrl, url.QueryEscape(dbname))
	sql := fmt.Sprintf("BACKUP DATABASE %s TO URL = '%s'", quoteName(dbname), backupUrl)
	tracelog.InfoLogger.Printf("staring backup database [%s] to %s", dbname, backupUrl)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] backup failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] backup successfully finished", dbname)
	}
	return err
}
