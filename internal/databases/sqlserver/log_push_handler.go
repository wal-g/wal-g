package sqlserver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
)

func HandleLogPush(ctx context.Context, dbnames []string, norecovery bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	folder, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)

	db, err := getSQLServerConnection(ctx)
	tracelog.ErrorLogger.FatalfOnError("failed to connect to SQLServer: %v", err)

	dbnames, err = getDatabasesToBackup(ctx, db, dbnames)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.ErrorLogger.FatalfOnError("failed to list databases to backup: %v", err)

	lock, err := RunOrReuseProxy(ctx, cancel, folder.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)
	defer lock.Close()

	builtinCompression := blob.UseBuiltinCompression()
	logBackupName := generateLogBackupName()
	err = runParallel(func(i int) error {
		return backupSingleLog(ctx, db, logBackupName, dbnames[i], builtinCompression, norecovery)
	}, len(dbnames), getDBConcurrency())
	tracelog.ErrorLogger.FatalfOnError("overall log backup failed: %v", err)

	tracelog.InfoLogger.Printf("log backup finished")
}

func backupSingleLog(ctx context.Context, db *sql.DB, backupName string, dbname string, builtinCompression bool, noRecovery bool) error {
	baseURL := getLogBackupURL(backupName, dbname)
	size, blobCount, err := estimateLogSize(ctx, db, dbname)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("database [%s] log size is %d, required blob count %d", dbname, size, blobCount)
	urls := buildBackupUrls(baseURL, blobCount)
	sql := fmt.Sprintf("BACKUP LOG %s TO %s", quoteName(dbname), urls)
	sql += fmt.Sprintf(" WITH FORMAT, MAXTRANSFERSIZE=%d", MaxTransferSize)
	if builtinCompression {
		sql += ", COMPRESSION"
	}
	if noRecovery {
		sql += ", NORECOVERY"
	}
	tracelog.InfoLogger.Printf("starting backup database [%s] log to %s", dbname, urls)
	tracelog.DebugLogger.Printf("SQL: %s", sql)
	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		tracelog.ErrorLogger.Printf("database [%s] log backup failed: %#v", dbname, err)
	} else {
		tracelog.InfoLogger.Printf("database [%s] log backup successfully finished", dbname)
	}
	return err
}
