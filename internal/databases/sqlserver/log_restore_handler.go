package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleLogRestore(backupName string, untilTS string, dbnames []string, fromnames []string, noRecovery bool) {
	ctx, cancel := context.WithCancel(context.Background())
	signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
	defer func() { _ = signalHandler.Close() }()

	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalOnError(err)

	sentinel := new(SentinelDto)
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection()
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, fromnames, err = getDatabasesToRestore(sentinel, dbnames, fromnames)
	tracelog.ErrorLogger.FatalfOnError("failed to list databases to restore logs: %v", err)

	lock, err := RunOrReuseProxy(ctx, cancel, folder)
	tracelog.ErrorLogger.FatalOnError(err)
	defer lock.Close()

	stopAt, err := utility.ParseUntilTS(untilTS)
	tracelog.ErrorLogger.FatalfOnError("invalid util timestamp: %v", err)

	logs, err := getLogsSinceBackup(folder, backup.Name, stopAt)
	tracelog.ErrorLogger.FatalfOnError("failed to list log backups: %v", err)

	err = runParallel(func(i int) error {
		dbname := dbnames[i]
		fromname := fromnames[i]
		if err != nil {
			return err
		}
		backupMetadata, err := GetBackupProperties(db, folder, false, backupName, fromname)
		if err != nil {
			return err
		}
		var dbBackupProperties *BackupProperties
		for _, dbBackupProperties = range backupMetadata {
			if dbBackupProperties.DatabaseName == fromname {
				break
			}
		}
		prevBackupFinishdate := dbBackupProperties.BackupFinishDate
		for _, logBackupName := range logs {
			ok, err := doesLogBackupContainDB(folder, logBackupName, fromname)
			if err != nil {
				return err
			}
			if !ok {
				// some log backup may not contain particular database in case
				// it was created or dropped between base backups
				tracelog.WarningLogger.Printf("log backup %s does not contains logs for database %s",
					logBackupName, fromname)
				continue
			}
			if prevBackupFinishdate.Before(stopAt) {
				CurrentBackupFinishdate, err := restoreSingleLog(ctx,
					db,
					folder,
					logBackupName,
					dbname,
					fromname,
					stopAt,
					prevBackupFinishdate)
				if err != nil {
					return err
				}
				prevBackupFinishdate = CurrentBackupFinishdate
			} else {
				break
			}
		}
		if !noRecovery {
			return recoverSingleDatabase(ctx, db, dbname)
		}
		return nil
	}, len(dbnames), getDBConcurrency())
	tracelog.ErrorLogger.FatalfOnError("overall log restore failed: %v", err)

	tracelog.InfoLogger.Printf("log restore finished")
}

func restoreSingleLog(ctx context.Context,
	db *sql.DB,
	folder storage.Folder,
	logBackupName string,
	dbname string,
	fromname string,
	stopAt time.Time,
	prevBackupFinishDate time.Time,
) (time.Time, error) {
	baseURL := getLogBackupURL(logBackupName, fromname)
	basePath := getLogBackupPath(logBackupName, fromname)
	blobs, err := listBackupBlobs(folder.GetSubFolder(basePath))
	if err != nil {
		return prevBackupFinishDate, err
	}
	urls := buildRestoreUrls(baseURL, blobs)

	logBackupFileProperties, err := GetBackupProperties(db, folder, true, logBackupName, fromname)
	if err != nil {
		return prevBackupFinishDate, err
	}
	if !prevBackupFinishDate.Before(stopAt) {
		tracelog.InfoLogger.Printf("Log Restore operation is inapplicable. STOPAT point left behind.")
		return prevBackupFinishDate, err
	}
	var sql string
	if logBackupFileProperties[0].BackupFinishDate.Before(stopAt) {
		sql = fmt.Sprintf("RESTORE LOG %s FROM %s WITH NORECOVERY",
			quoteName(dbname), urls)
	} else {
		sql = fmt.Sprintf("RESTORE LOG %s FROM %s WITH NORECOVERY, STOPAT = '%s'",
			quoteName(dbname), urls, stopAt.Format(TimeSQLServerFormat))
	}
	tracelog.InfoLogger.Printf("starting restore database [%s] log from %s", dbname, urls)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] log restore failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] log restore succefully finished", dbname)
	}
	return logBackupFileProperties[0].BackupFinishDate, err
}
