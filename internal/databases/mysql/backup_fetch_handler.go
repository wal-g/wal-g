package mysql

import (
	"io"
	"os/exec"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd) {
	backupName, err := targetBackupSelector.Select(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s)\n", backupName)

	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

	// Fetch Sentinel
	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v\n", err)

	// Fetch Backup
	streamFetcher := internal.GetCommandStreamFetcher(restoreCmd, getBackupFetcher(backup, sentinel))
	streamFetcher(folder, backup)

	// Prepare Backup
	if prepareCmd != nil {
		err := prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}

func getBackupFetcher(backup internal.Backup, sentinel StreamSentinelDto) internal.StreamFeature {
	switch sentinel.Type {
	case SplitMergeStreamBackup:
		var partitions = sentinel.Partitions
		var blockSize = sentinel.BLockSize
		var compression = sentinel.Compression
		return func(backup internal.Backup, writer io.WriteCloser) error {
			return internal.DownloadAndDecompressSplittedStream(backup, partitions, int(blockSize), compression, writer)
		}
	case SingleStreamStreamBackup, "":
		return internal.DownloadAndDecompressStream
	}
	tracelog.ErrorLogger.Fatalf("Unknown backup type %s", sentinel.Type)
	return nil // unreachable
}
