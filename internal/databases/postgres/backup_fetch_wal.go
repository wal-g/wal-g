package postgres

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var backupStartWALPattern = regexp.MustCompile(`(?m)^START WAL LOCATION: [0-9A-Fa-f]+/[0-9A-Fa-f]+ \(file ([0-9A-Fa-f]{24})\)\r?$`)

// WithWALFetch fetches WAL for the selected backup after either unpack implementation finishes.
// Delta ancestors supply data files, but recovery starts at the selected backup's label.
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
		return fmt.Errorf("read backup label for required WAL range: %w", err)
	}
	timeline, first, last, err := backupWALRange(sentinel, label)
	if err != nil {
		return err
	}
	walDir, err := getWalDirName(dbDataDirectory)
	if err != nil {
		return err
	}
	// Keep the destinations selected by backup-fetch, while allowing WAL reads to
	// search across those destinations just as wal-fetch does.
	reader := internal.NewFolderReader(multistorage.SetPolicies(rootFolder, policies.MergeAllStorages)).SubFolder(utility.WalPath)
	if timeline > 1 {
		history := fmt.Sprintf(walHistoryFileFormat, timeline)
		if err := fetchBackupWALFile(ctx, reader, history, walDir, -1); err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Printf("Fetching backup WAL from %s through %s", first.GetFilename(timeline), last.GetFilename(timeline))
	for segment := first; ; segment = segment.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fetchBackupWALFile(ctx, reader, segment.GetFilename(timeline), walDir, int64(WalSegmentSize)); err != nil {
			return err
		}
		if segment == last {
			return nil
		}
	}
}

func backupWALRange(sentinel BackupSentinelDto, label []byte) (uint32, WalSegmentNo, WalSegmentNo, error) {
	// The label is authoritative, including when START WAL LOCATION is exactly
	// a segment boundary. Older sentinels do not contain WAL filenames.
	match := backupStartWALPattern.FindSubmatch(label)
	if match == nil {
		return 0, 0, 0, fmt.Errorf("backup label has no valid START WAL LOCATION")
	}
	timeline, first, err := ParseWALFilename(string(match[1]))
	if err != nil || timeline == 0 {
		return 0, 0, 0, fmt.Errorf("invalid backup start WAL filename %q", match[1])
	}
	var last uint64
	if sentinel.EndWalFileName != "" {
		var endTimeline uint32
		endTimeline, last, err = ParseWALFilename(sentinel.EndWalFileName)
		if err != nil || endTimeline != timeline {
			return 0, 0, 0, fmt.Errorf("invalid backup end WAL filename %q for timeline %d", sentinel.EndWalFileName, timeline)
		}
	} else {
		if sentinel.BackupFinishLSN == nil || *sentinel.BackupFinishLSN == 0 {
			return 0, 0, 0, fmt.Errorf("backup metadata has no WAL end filename or finish LSN")
		}
		// BackupFinishLSN points just after the last required record.
		last = GetSegmentNoFromLsn(*sentinel.BackupFinishLSN - 1)
	}
	if first > last {
		return 0, 0, 0, fmt.Errorf("backup WAL range ends before it starts")
	}
	return timeline, WalSegmentNo(first), WalSegmentNo(last), nil
}

func fetchBackupWALFile(ctx context.Context, reader internal.StorageFolderReader, name, walDir string, expectedSize int64) error {
	if err := downloadBackupWALFile(ctx, reader, name, walDir, expectedSize); err != nil {
		return fmt.Errorf("fetch required WAL file %s: %w", name, err)
	}
	return nil
}

func downloadBackupWALFile(ctx context.Context, reader internal.StorageFolderReader, name, walDir string, expectedSize int64) error {
	source, err := internal.DownloadAndDecompressStorageFile(ctx, reader, name)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(walDir, ".walg-fetch-wal-*")
	if err != nil {
		_ = source.Close()
		return err
	}
	defer os.Remove(file.Name())
	// Consume the decoder through EOF before publishing, including trailing
	// checksums. Never leave a failed or truncated download under a WAL filename.
	n, copyErr := io.Copy(file, source)
	sourceErr := source.Close()
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if sourceErr != nil {
		return sourceErr
	}
	if closeErr != nil {
		return closeErr
	}
	if expectedSize >= 0 && n != expectedSize {
		return fmt.Errorf("decoded size is %d bytes, expected %d (check WALG_PG_WAL_SIZE)", n, expectedSize)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(walDir, name))
}
