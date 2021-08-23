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
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
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

	bs, err := blob.NewServer(folder)
	tracelog.ErrorLogger.FatalfOnError("proxy create error: %v", err)

	lock, err := bs.AcquireLock()
	tracelog.ErrorLogger.FatalOnError(err)
	defer func() { tracelog.ErrorLogger.PrintOnError(lock.Unlock()) }()

	err = bs.RunBackground(ctx, cancel)
	tracelog.ErrorLogger.FatalfOnError("proxy run error: %v", err)

	stopAt, err := utility.ParseUntilTS(untilTS)
	tracelog.ErrorLogger.FatalfOnError("invalid util timestamp: %v", err)

	logs, err := getLogsSinceBackup(folder, backup.Name, stopAt)
	tracelog.ErrorLogger.FatalfOnError("failed to list log backups: %v", err)

	err = runParallel(func(i int) error {
		dbname := dbnames[i]
		fromname := fromnames[i]
		baseURL := getDatabaseBackupURL(backupName, fromname)
		basePath := getDatabaseBackupPath(backupName, fromname)
		blobs, err := listBackupBlobs(folder.GetSubFolder(basePath))
		if err != nil {
			return err
		}
		urls := buildRestoreUrls(baseURL, blobs)
		backupMetadata, err := ListBackupProperties(db, urls, baseURL)
		if err != nil {
			return err
		}
		var dbBackupMetadata *BackupProperties
		for _, dbBackupMetadata = range backupMetadata {
			if dbBackupMetadata.DatabaseName == fromname {
				break
			}
		}
		prevBackupFinishdate := dbBackupMetadata.BackupFinishDate
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
			isLastOne, CurrentBackupFinishdate, err := restoreSingleLog(ctx,
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
			if isLastOne {
				break
			}
			prevBackupFinishdate = CurrentBackupFinishdate
		}
		if !noRecovery {
			return recoverSingleDatabase(ctx, db, dbname)
		}
		return nil
	}, len(dbnames))
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
) (bool, time.Time, error) {
	var isLastOne bool
	baseURL := getLogBackupURL(logBackupName, fromname)
	basePath := getLogBackupPath(logBackupName, fromname)
	blobs, err := listBackupBlobs(folder.GetSubFolder(basePath))
	if err != nil {
		return isLastOne, prevBackupFinishDate, err
	}
	urls := buildRestoreUrls(baseURL, blobs)

	logBackupFileMetadata, err := ListBackupProperties(db, urls, logBackupName)
	if err != nil {
		return isLastOne, prevBackupFinishDate, err
	}
	if !prevBackupFinishDate.Before(stopAt) {
		tracelog.InfoLogger.Printf("Log Restore operation is inapplicable. STOPAT point left behind.")
		return true, prevBackupFinishDate, err
	}
	var sql string
	if logBackupFileMetadata[0].BackupFinishDate.Before(stopAt) {
		sql = fmt.Sprintf("RESTORE LOG %s FROM %s WITH NORECOVERY",
			quoteName(dbname), urls)
	} else {
		sql = fmt.Sprintf("RESTORE LOG %s FROM %s WITH NORECOVERY, STOPAT = '%s'",
			quoteName(dbname), urls, stopAt.Format(TimeSQLServerFormat))
		isLastOne = true
	}
	tracelog.InfoLogger.Printf("starting restore database [%s] log from %s", dbname, urls)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] log restore failed: %v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] log restore succefully finished", dbname)
	}
	return isLastOne, logBackupFileMetadata[0].BackupFinishDate, err
}
