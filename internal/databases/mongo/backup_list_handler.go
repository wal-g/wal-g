package mongo

import (
	"io"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"

	"github.com/wal-g/tracelog"
)

// HandleBackupsList prints current backups.
func HandleBackupsList(downloader archive.Downloader, listing archive.BackupListing, output io.Writer, verbose bool) error {
	backupTimes, _, err := downloader.ListBackups()
	if err != nil {
		return err
	}

	if len(backupTimes) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return nil
	}

	if !verbose {
		return listing.Names(backupTimes, output)
	}

	backups, err := downloader.LoadBackups(archive.BackupNamesFromBackupTimes(backupTimes))
	if err != nil {
		return err
	}

	return listing.Backups(backups, output)
}
