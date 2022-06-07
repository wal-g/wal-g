package mongo

import (
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
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

	_, _, err = HandleBackupsPurge(backupTimes, downloader, purger, opts)
	if err != nil {
		return err
	}

	if opts.purgeOplog {
		// TODO: fix error if retainBackups is empty
		if err := HandleOplogPurge(downloader, purger, opts.retainAfter, opts.dryRun); err != nil {
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
func HandleBackupsPurge(backupTimes []internal.BackupTime,
	downloader archive.Downloader,
	purger archive.Purger,
	opts PurgeSettings) (purge, retain []*models.Backup, err error) {
	if len(backupTimes) == 0 { // TODO: refactor && support oplog purge even if backups do not exist
		tracelog.InfoLogger.Println("No backups found")
		return nil, nil, nil
	}

	backups, err := downloader.LoadBackups(archive.BackupNamesFromBackupTimes(backupTimes))
	if err != nil {
		return nil, nil, err
	}

	timedBackups := archive.MongoModelToTimedBackup(backups)

	internal.SortTimedBackup(timedBackups)
	purgeBackups, retainBackups, err := internal.SplitPurgingBackups(timedBackups, opts.retainCount, opts.retainAfter)

	if err != nil {
		return nil, nil, err
	}

	purge, retain = archive.SplitMongoBackups(backups, purgeBackups, retainBackups)
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
