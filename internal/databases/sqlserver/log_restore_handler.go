package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
)

func HandleLogRestore(backupName string, untilTs string, dbnames []string, noRecovery bool) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	backup, err := internal.BackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalOnError(err)

	sentinel := new(SentinelDto)
	err = internal.FetchStreamSentinel(backup, sentinel)
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection()
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, err = getDatabasesToRestore(sentinel, dbnames)
	tracelog.ErrorLogger.FatalfOnError("failed to list databases to restore logs: %v", err)

	bs, err := blob.NewServer(folder)
	tracelog.ErrorLogger.FatalfOnError("proxy create error: %v", err)

	err = bs.RunBackground(ctx, cancel)
	tracelog.ErrorLogger.FatalfOnError("proxy run error: %v", err)

	stopAt, err := utility.ParseUntilTs(untilTs)
	tracelog.ErrorLogger.FatalfOnError("invalid util timestamp: %v", err)

	logs, err := getLogsSinceBackup(folder, backup.Name, stopAt)
	tracelog.ErrorLogger.FatalfOnError("failed to list log backups: %v", err)

	err = runParallel(func(dbname string) error {
		for _, logBackupName := range logs {
			ok, err := doesLogBackupContainDb(folder, logBackupName, dbname)
			if err != nil {
				return err
			}
			if !ok {
				// some log backup may not contain particular database in case
				// it was created or dropped between base backups
				tracelog.WarningLogger.Printf("log backup %s does not contains logs for database %s",
					logBackupName, dbname)
				continue
			}
			baseUrl := getLogBackupUrl(logBackupName)
			err = restoreSingleLog(ctx, db, baseUrl, dbname, stopAt)
			if err != nil {
				return err
			}
		}
		if !noRecovery {
			return recoverSingleDatabase(ctx, db, dbname)
		}
		return nil
	}, dbnames)
	tracelog.ErrorLogger.FatalfOnError("overall log restore failed: %v", err)

	tracelog.InfoLogger.Printf("log restore finished")
}

func restoreSingleLog(ctx context.Context, db *sql.DB, baseUrl string, dbname string, stopAt time.Time) error {
	backupUrl := fmt.Sprintf("%s/%s", baseUrl, url.QueryEscape(dbname))
	sql := fmt.Sprintf("RESTORE LOG %s FROM URL = '%s' WITH NORECOVERY, STOPAT = '%s'",
		quoteName(dbname), backupUrl, stopAt.Format(TimeSQLServerFormat))
	tracelog.InfoLogger.Printf("starting restore database [%s] log from %s", dbname, backupUrl)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] log restore failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] log restore succefully finished", dbname)
	}
	return err
}
