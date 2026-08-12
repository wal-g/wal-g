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

	internal.DeleteJournalInfo(ctx, storage.RootFolder(), backupName, models.OplogArchBasePath,
		internal.NewJournalDirSizeCalculator(), !dryRun)
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
