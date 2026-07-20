package mongo

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func purgeJournalInfo(ctx context.Context, backupName string, dryRun bool) {
	storage, err := internal.ConfigureStorage(ctx)
	if err != nil {
		tracelog.WarningLogger.Printf("Can't configure storage: %+v", err)
		return
	}

	journalInfo, err := internal.NewJournalInfo(
		ctx,
		backupName,
		storage.RootFolder(),
		models.OplogArchBasePath,
	)
	// Backup could be created without journal
	if err != nil {
		tracelog.WarningLogger.Printf("Can't find the journal info: %+v", err)
		return
	}

	if dryRun {
		tracelog.InfoLogger.Printf("About to delete journal info: %+v", journalInfo)
		return
	}

	err = journalInfo.Delete(ctx, storage.RootFolder())
	if err != nil {
		tracelog.ErrorLogger.Print(err)
	} else {
		tracelog.InfoLogger.Printf("Deleted journal info: %+v", journalInfo)
	}
}

// HandleBackupDelete deletes backup.
func HandleBackupDelete(ctx context.Context, backupName string, downloader archive.Downloader, purger archive.Purger, dryRun bool) error {
	backup, err := downloader.BackupMeta(ctx, backupName)
	if err != nil {
		return err
	}

	if dryRun {
		tracelog.InfoLogger.Printf("Skipping backup deletion due to dry-run: %+v", backup)
		return nil
	}

	if err := purger.DeleteBackups(ctx, []*models.Backup{backup}); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Backup was deleted: %+v", backup)
	purgeJournalInfo(ctx, backupName, dryRun)
	return nil
}
