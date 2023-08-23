package redis

import (
	"fmt"
	"sort"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
)

type PurgeSettings struct {
	retainCount  *int
	retainAfter  *time.Time
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
func HandlePurge(backupsPath string, setters ...PurgeOption) error {
	opts := PurgeSettings{dryRun: true}
	for _, setter := range setters {
		setter(&opts)
	}

	folder, err := internal.ConfigureFolder()
	if err != nil {
		return err
	}

	backupFolder := folder.GetSubFolder(backupsPath)

	backupTimes, garbage, err := internal.GetBackupsAndGarbage(backupFolder)
	if err != nil {
		return err
	}

	_, _, err = HandleBackupsDelete(backupTimes, backupFolder, opts)
	if err != nil {
		return err
	}

	if opts.purgeGarbage {
		tracelog.InfoLogger.Printf("Garbage prefixes in backups folder: %v", garbage)
		if !opts.dryRun {
			if err := internal.DeleteGarbage(backupFolder, garbage); err != nil {
				return err
			}
		}
	}

	return nil
}

// HandleBackupsDelete delete backups according to settings
func HandleBackupsDelete(backupTimes []internal.BackupTime,
	folder storage.Folder,
	opts PurgeSettings) (purge, retain []archive.Backup, err error) {
	if len(backupTimes) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return []archive.Backup{}, []archive.Backup{}, nil
	}

	backups, err := LoadBackups(folder, BackupNamesFromBackupTimes(backupTimes))
	if err != nil {
		return nil, nil, err
	}

	timedBackup := archive.RedisModelToTimedBackup(backups)

	internal.SortTimedBackup(timedBackup)
	purgeBackups, retainBackups, err := internal.SplitPurgingBackups(timedBackup, opts.retainCount, opts.retainAfter)
	if err != nil {
		return nil, nil, err
	}

	purge, retain = archive.SplitRedisBackups(backups, purgeBackups, retainBackups)

	purgeFiles := BackupNamesFromBackups(purge)

	tracelog.InfoLogger.Printf("Backups selected to be deleted: %v", purgeFiles)
	tracelog.InfoLogger.Printf("Backups selected to be retained: %v", BackupNamesFromBackups(retain))

	if !opts.dryRun {
		if err := internal.DeleteBackups(folder, purgeFiles); err != nil {
			return nil, nil, err
		}
		tracelog.InfoLogger.Printf("Backups were purged: deleted: %d, retained: %v", len(purge), len(retain))
	}
	return purge, retain, nil
}

// LoadBackups downloads backups metadata
func LoadBackups(folder storage.Folder, names []string) ([]archive.Backup, error) {
	backups := make([]archive.Backup, 0, len(names))
	for _, name := range names {
		backup, err := BackupMeta(folder, name)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].FinishLocalTime.After(backups[j].FinishLocalTime)
	})
	return backups, nil
}

func BackupMeta(folder storage.Folder, name string) (archive.Backup, error) {
	backup, err := internal.NewBackup(folder, name)
	if err != nil {
		return archive.Backup{}, err
	}
	var sentinel archive.Backup
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		return archive.Backup{}, fmt.Errorf("can not fetch stream sentinel: %w", err)
	}
	if sentinel.BackupName == "" {
		sentinel.BackupName = name
	}
	return sentinel, nil
}

// BackupNamesFromBackupTimes forms list of backup names from BackupTime
func BackupNamesFromBackupTimes(backups []internal.BackupTime) []string {
	names := make([]string, 0, len(backups))
	for _, b := range backups {
		names = append(names, b.BackupName)
	}
	return names
}

// BackupNamesFromBackups forms list of backup names from Backups
func BackupNamesFromBackups(backups []archive.Backup) []string {
	names := make([]string, 0, len(backups))
	for idx := range backups {
		names = append(names, backups[idx].BackupName)
	}
	return names
}
