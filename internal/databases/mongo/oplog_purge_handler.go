package mongo

import (
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func LoadBackups(downloader archive.Downloader) ([]models.Backup, error) {
	backupTimes, _, err := downloader.ListBackups()
	if err != nil {
		return nil, err
	}
	if len(backupTimes) == 0 {
		return []models.Backup{}, nil
	}
	return downloader.LoadBackups(archive.BackupNamesFromBackupTimes(backupTimes))
}

// HandleOplogPurge delete oplog archives according to settings
func HandleOplogPurge(downloader archive.Downloader, purger archive.Purger, retainAfter *time.Time, dryRun bool) error {
	archives, err := downloader.ListOplogArchives()
	if err != nil {
		return fmt.Errorf("can not load oplog archives: %+v", err)
	}
	if len(archives) == 0 {
		return nil
	}
	backups, err := LoadBackups(downloader)
	if err != nil {
		return fmt.Errorf("can not load backups: %+v", err)
	}
	if len(backups) == 0 {
		// TODO: should we purge all logs then? fail here for now
		return fmt.Errorf("can not find any existed backups")
	}

	pitrBackup, err := archive.OldestBackupAfterTime(backups, *retainAfter) // TODO: make new setting - PITR point
	if err != nil {
		return err
	}
	retainArchivesAfterTS := pitrBackup.MongoMeta.Before.LastMajTS

	tracelog.DebugLogger.Printf("Oldest backup in PITR interval is %+v\n", pitrBackup)
	tracelog.DebugLogger.Printf("Oplog archives newer than %+v will be retained\n", retainArchivesAfterTS)
	tracelog.DebugLogger.Printf("Oplog archives included into backups time interval will be retained: %v\n", backups)

	purgeArchives := archive.SelectPurgingOplogArchives(archives, backups, &retainArchivesAfterTS)
	tracelog.DebugLogger.Printf("Oplog archives selected to be deleted: %v", purgeArchives)
	if !dryRun {
		if err := purger.DeleteOplogArchives(purgeArchives); err != nil {
			return fmt.Errorf("can not purge oplog archives: %+v", err)
		}
		tracelog.InfoLogger.Printf("Oplog archives were purged: %d", len(purgeArchives))
	}
	return nil
}
