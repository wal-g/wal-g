package redis

import (
	"context"
	"os"
	"slices"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDetailedBackupList(ctx context.Context, folder storage.Folder, pretty bool, json bool) {
	backups, err := internal.GetBackups(ctx, folder)
	err = internal.FilterOutNoBackupFoundError(err, json)
	tracelog.ErrorLogger.FatalOnError(err)

	backupDetails, err := GetBackupDetails(ctx, folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = &backupDetails[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}

func GetBackupDetails(ctx context.Context, folder storage.Folder, backups []internal.BackupTime) ([]archive.Backup, error) {
	backupDetails := make([]archive.Backup, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := archive.SentinelWithoutExistenceCheck(ctx, folder, backups[i].BackupName)
		if err != nil {
			return nil, err
		}
		if err = archive.EnrichWithAttachedTS(ctx, folder, &details); err != nil {
			return nil, err
		}
		backupDetails = append(backupDetails, details)
	}

	slices.SortFunc(backupDetails, func(a, b archive.Backup) int {
		return a.FinishLocalTime.Compare(b.FinishLocalTime)
	})

	return backupDetails, nil
}
