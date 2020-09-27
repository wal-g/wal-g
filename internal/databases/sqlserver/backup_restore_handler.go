package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupRestore(backupName string, dbnames []string, noRecovery bool) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalOnError(err)

	sentinel := new(SentinelDto)
	err = internal.FetchStreamSentinel(backup, sentinel)
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection()
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, err = getDatabasesToRestore(sentinel, dbnames)
	tracelog.ErrorLogger.FatalfOnError("failed to list databases to restore: %v", err)

	bs, err := blob.NewServer(folder)
	tracelog.ErrorLogger.FatalfOnError("proxy create error: %v", err)

	err = bs.RunBackground(ctx, cancel)
	tracelog.ErrorLogger.FatalfOnError("proxy run error: %v", err)

	backupName = backup.Name
	baseUrl := getDatabaseBackupUrl(backupName)

	err = runParallel(func(dbname string) error {
		err := restoreSingleDatabase(ctx, db, baseUrl, dbname)
		if err != nil {
			return err
		}
		if !noRecovery {
			return recoverSingleDatabase(ctx, db, dbname)
		}
		return nil
	}, dbnames)
	tracelog.ErrorLogger.FatalfOnError("overall restore failed: %v", err)

	tracelog.InfoLogger.Printf("restore finished")
}

func restoreSingleDatabase(ctx context.Context, db *sql.DB, baseUrl string, dbname string) error {
	backupUrl := fmt.Sprintf("%s/%s", baseUrl, url.QueryEscape(dbname))
	sql := fmt.Sprintf("RESTORE DATABASE %s FROM URL = '%s' WITH REPLACE, NORECOVERY", quoteName(dbname), backupUrl)
	tracelog.InfoLogger.Printf("starting restore database [%s] from %s", dbname, backupUrl)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] restore failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] restore succefully finished", dbname)
	}
	return err
}

func recoverSingleDatabase(ctx context.Context, db *sql.DB, dbname string) error {
	sql := fmt.Sprintf("RESTORE DATABASE %s WITH RECOVERY", quoteName(dbname))
	tracelog.InfoLogger.Printf("recovering database [%s]", dbname)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] recovery failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] recovery succefully finished", dbname)
	}
	return err
}
