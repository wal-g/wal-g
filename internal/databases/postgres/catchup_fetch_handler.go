package postgres

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCatchupFetch is invoked to perform wal-g catchup-fetch
func HandleCatchupFetch(folder storage.Folder, dbDirectory, backupName string, useNewUnwrap bool) {
	dbDirectory = utility.ResolveSymlink(dbDirectory)

	backup, err := internal.GetBackupByName(backupName, utility.CatchupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed get backup by name: %v", err)

	pgBackup := ToPgBackup(backup)
	filesToUnwrap, err := pgBackup.GetFilesToUnwrap("")
	tracelog.ErrorLogger.FatalfOnError("Failed get files to unwrap from backup: %v", err)

	_, _, err = pgBackup.GetSentinelAndFilesMetadata()
	tracelog.ErrorLogger.FatalfOnError("Failed get backup sentinel: %v", err)

	// testing the new unwrap implementation
	if useNewUnwrap {
		_, err = pgBackup.unwrapNew(dbDirectory, filesToUnwrap, true, false, ExtractProviderImpl{})
	} else {
		err = pgBackup.unwrapOld(dbDirectory, filesToUnwrap, true, ExtractProviderImpl{})
	}

	tracelog.ErrorLogger.FatalfOnError("Failed unwrap backup: %v", err)
}
