package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var backupStartWALPattern = regexp.MustCompile(`(?m)^START WAL LOCATION: [0-9A-Fa-f]+/[0-9A-Fa-f]+ \(file ([0-9A-Fa-f]{24})\)\r?$`)

// WithWALFetch fetches WAL for the selected backup after either unpack implementation finishes.
// Delta ancestors supply data files, but recovery uses the selected backup's LSN range.
func WithWALFetch(fetcher internal.Fetcher, dbDataDirectory string) internal.Fetcher {
	return func(ctx context.Context, rootFolder storage.Folder, backup internal.Backup) {
		fetcher(ctx, rootFolder, backup)
		err := FetchBackupWAL(ctx, rootFolder, ToPgBackup(backup), dbDataDirectory)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup WAL: %v\n", err)
	}
}

// FetchBackupWAL downloads the WAL needed to make the selected backup consistent.
// It uses direct object reads, without enumerating the WAL archive.
func FetchBackupWAL(ctx context.Context, rootFolder storage.Folder, backup Backup, dbDataDirectory string) error {
	sentinel, err := backup.GetSentinel(ctx)
	if err != nil {
		return err
	}
	label, err := os.ReadFile(filepath.Join(dbDataDirectory, BackupLabelFilename))
	if err != nil {
		return fmt.Errorf("read backup label for required WAL timeline: %w", err)
	}
	timeline, first, last, err := backupWALRange(sentinel, label)
	if err != nil {
		return err
	}
	walDir, err := getWalDirName(dbDataDirectory)
	if err != nil {
		return err
	}

	reader := internal.NewFolderReader(multistorage.SetPolicies(rootFolder, policies.MergeAllStorages))
	if timeline > 1 {
		history := fmt.Sprintf(walHistoryFileFormat, timeline)
		if err := HandleWALFetch(ctx, reader, history, fmt.Sprintf("%s/%s", walDir, history), NopPrefetcher{}); err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Printf("Fetching backup WAL from %s through %s", first.GetFilename(timeline), last.GetFilename(timeline))
	for segment := first; ; segment = segment.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := HandleWALFetch(ctx,
			reader,
			segment.GetFilename(timeline),
			fmt.Sprintf("%s/%s", walDir, segment.GetFilename(timeline)),
			NopPrefetcher{})
		if err != nil {
			return err
		}
		if segment == last {
			return nil
		}
	}
}

func backupWALRange(sentinel BackupSentinelDto, label []byte) (uint32, WalSegmentNo, WalSegmentNo, error) {
	match := backupStartWALPattern.FindSubmatch(label)
	if match == nil {
		return 0, 0, 0, fmt.Errorf("backup label has no valid START WAL LOCATION")
	}
	timeline, _, err := ParseWALFilename(string(match[1]))
	if err != nil || timeline == 0 {
		return 0, 0, 0, fmt.Errorf("invalid backup start WAL filename %q", match[1])
	}
	if sentinel.BackupStartLSN == nil || sentinel.BackupFinishLSN == nil {
		return 0, 0, 0, fmt.Errorf("backup metadata has no valid LSN boundaries")
	}
	start, finish := *sentinel.BackupStartLSN, *sentinel.BackupFinishLSN
	if start == 0 || finish <= start {
		return 0, 0, 0, fmt.Errorf("invalid backup LSN boundaries: start %s must be nonzero and less than finish %s", start, finish)
	}
	// The start LSN is inclusive, even at an exact segment boundary. The finish
	// LSN points just after the last required record.
	first := NewWalSegmentNo(start)
	last := NewWalSegmentNo(finish - 1)
	return timeline, first, last, nil
}
