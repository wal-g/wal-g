package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
)

const DatabaseBackupItem = "DATABASE"
const LogBackupItem = "LOG"

func HandleBackupPush(dbnames []string, updateLatest bool, compression bool) {
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
	var backupName string
	var sentinel *SentinelDto
	if updateLatest {
		backup, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
		tracelog.ErrorLogger.FatalfOnError("can't find latest backup: %v", err)
		backupName = backup.Name
		sentinel = new(SentinelDto)
		err = internal.FetchStreamSentinel(backup, sentinel)
		tracelog.ErrorLogger.FatalOnError(err)
		sentinel.Databases = uniq(append(sentinel.Databases, dbnames...))
	} else {
		backupName = generateDatabaseBackupName()
		sentinel = &SentinelDto{
			Server:         server,
			Databases:      dbnames,
			StartLocalTime: timeStart,
		}
	}
	baseUrl := getDatabaseBackupUrl(backupName)

	err = runParallel(func(dbname string) error {
		return backupSingleItem(DatabaseBackupItem, ctx, db, baseUrl, dbname, compression)
	}, dbnames)
	tracelog.ErrorLogger.FatalfOnError("overall backup failed: %v", err)

	uploader := internal.NewUploader(nil, folder.GetSubFolder(utility.BaseBackupPath))
	tracelog.InfoLogger.Printf("uploading sentinel: %s", sentinel)
	err = internal.UploadSentinel(uploader, sentinel, backupName)
	tracelog.ErrorLogger.FatalfOnError("failed to save sentinel: %v", err)

	tracelog.InfoLogger.Printf("backup finished")
}

func backupSingleItem(itemName string, ctx context.Context, db *sql.DB,
	baseUrl string, dbname string, compression bool) error {
	if itemName != DatabaseBackupItem && itemName != LogBackupItem {
		return errors.New("unknown backup item")
	}

	backupUrl := fmt.Sprintf("%s/%s", baseUrl, url.QueryEscape(dbname))
	sql := fmt.Sprintf("BACKUP %s %s TO URL = '%s' WITH FORMAT", itemName, quoteName(dbname), backupUrl)
	if compression {
		sql += ", COMPRESSION"
	}
	tracelog.InfoLogger.Printf("starting backup database [%s] %s to %s", dbname, itemName, backupUrl)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] %s backup failed: %#v", dbname, itemName, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] %s backup successfully finished", dbname, itemName)
	}
	return err
}
