package mongo

import (
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/tracelog"
)

type PurgeSettings struct {
	retainCount  *int
	retainAfter  *time.Time
	purgeOplog   bool
	purgeGarbage bool
	dryRun       bool
}

type PurgeOption func(*PurgeSettings)

// PurgeRetainAfter ...
func PurgeRetainAfter(retainAfter time.Time) PurgeOption {
	return func(args *PurgeSettings) {
		args.retainAfter = &retainAfter
	}
}

// PurgeRetainCount ...
func PurgeRetainCount(retainCount int) PurgeOption {
	return func(args *PurgeSettings) {
		args.retainCount = &retainCount
	}
}

// PurgeOplog ...
func PurgeOplog(purgeOplog bool) PurgeOption {
	return func(args *PurgeSettings) {
		args.purgeOplog = purgeOplog
	}
}

// PurgeGarbage ...
func PurgeGarbage(purgeGarbage bool) PurgeOption {
	return func(args *PurgeSettings) {
		args.purgeGarbage = purgeGarbage
	}
}

// PurgeDryRun ...
func PurgeDryRun(dryRun bool) PurgeOption {
	return func(args *PurgeSettings) {
		args.dryRun = dryRun
	}
}

// HandlePurge delete backups and oplog archives according to settings
func HandlePurge(downloader archive.Downloader, purger archive.Purger, setters ...PurgeOption) error {
	opts := PurgeSettings{purgeOplog: false, dryRun: true}
	for _, setter := range setters {
		setter(&opts)
	}

	backupTimes, garbage, err := downloader.ListBackups()
	if err != nil {
		return err
	}

	_, retainBackups, err := HandleBackupsPurge(backupTimes, downloader, purger, opts)
	if err != nil {
		return err
	}

	if opts.purgeOplog {
		// TODO: fix error if retainBackups is empty
		if _, err := HandleOplogArchivesPurge(downloader, purger, retainBackups, opts); err != nil {
			return err
		}
	}

	if opts.purgeGarbage {
		tracelog.InfoLogger.Printf("Garbage prefixes in backups folder: %v", garbage)
		if !opts.dryRun {
			if err := purger.DeleteGarbage(garbage); err != nil {
				return err
			}
		}
	}

	return nil
}

// HandleBackupsPurge delete backups according to settings
func HandleBackupsPurge(backupTimes []internal.BackupTime, downloader archive.Downloader, purger archive.Purger, opts PurgeSettings) (purge, retain []archive.Backup, err error) {
	if len(backupTimes) == 0 { // TODO: refactor && support oplog purge even if backups do not exist
		tracelog.InfoLogger.Println("No backups found")
		return []archive.Backup{}, []archive.Backup{}, nil
	}

	backups, err := downloader.LoadBackups(archive.BackupNamesFromBackupTimes(backupTimes))
	if err != nil {
		return nil, nil, err
	}

	purge, retain, err = archive.SplitPurgingBackups(backups, opts.retainCount, opts.retainAfter)
	if err != nil {
		return nil, nil, err
	}
	tracelog.InfoLogger.Printf("Backups selected to be deleted: %v", archive.BackupNamesFromBackups(purge))
	tracelog.InfoLogger.Printf("Backups selected to be retained: %v", archive.BackupNamesFromBackups(retain))

	if !opts.dryRun {
		if err := purger.DeleteBackups(purge); err != nil {
			return nil, nil, err
		}
		tracelog.InfoLogger.Printf("Backups were purged: deleted: %d, retained: %v", len(purge), len(retain))
	}
	return purge, retain, nil
}

// HandleOplogArchivesPurge delete oplog archives according to settings
func HandleOplogArchivesPurge(downloader archive.Downloader, purger archive.Purger, backups []archive.Backup, opts PurgeSettings) (purge []models.Archive, err error) {
	archives, err := downloader.ListOplogArchives()
	if err != nil {
		return nil, err
	}
	if len(archives) == 0 {
		return []models.Archive{}, nil
	}

	purgeBeforeTS, err := archive.LastKnownInBackupTS(backups)
	if err != nil {
		return nil, err
	}

	tracelog.InfoLogger.Printf("Oplog archives will be purged if start_ts < %v", purgeBeforeTS)
	purgeArchives := archive.SplitPurgingOplogArchives(archives, purgeBeforeTS)
	tracelog.DebugLogger.Printf("Oplog archives selected to be deleted: %v", purgeArchives)

	if !opts.dryRun {
		if err := purger.DeleteOplogArchives(purgeArchives); err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("Oplog archives were purged: deleted: %d", len(purgeArchives))
	}
	return purgeArchives, nil
}
