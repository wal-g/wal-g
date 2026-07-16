package ts

import (
	"context"
	"fmt"
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/databases/redis/pin"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type PushArgs struct {
	Uploader   internal.Uploader
	SourceDir  string
	PinFolder  string
	DataPrefix string
	BackupID   string
	BackupPath string
	Permanent  bool
}

// Push uploads a recursively pinned tiered-storage tree and writes its sentinel.
func Push(ctx context.Context, args PushArgs) (*archive.Backup, error) {
	sourceInfo, err := os.Stat(args.SourceDir)
	if err != nil {
		return nil, fmt.Errorf("stat ts source directory %s: %w", args.SourceDir, err)
	}
	if !sourceInfo.IsDir() {
		return nil, fmt.Errorf("ts source %s is not a directory", args.SourceDir)
	}
	if err := os.MkdirAll(args.PinFolder, 0o700); err != nil {
		return nil, fmt.Errorf("create ts pin folder %s: %w", args.PinFolder, err)
	}
	if err := pin.ValidateSameFilesystem(args.SourceDir, args.PinFolder); err != nil {
		return nil, err
	}
	pinRoot, err := os.MkdirTemp(args.PinFolder, "ts-")
	if err != nil {
		return nil, fmt.Errorf("create ts pin mirror: %w", err)
	}
	defer os.RemoveAll(pinRoot)
	pinner := pin.NewFilesPinner(pinRoot)
	pinnedPaths, err := pinner.PinTree(args.SourceDir)
	if err != nil {
		return nil, err
	}
	defer pinner.Unpin()

	concurrentUploader, err := internal.CreateConcurrentUploader(ctx, internal.CreateConcurrentUploaderArgs{
		Uploader:   args.Uploader,
		BackupName: args.DataPrefix,
		Directory:  pinRoot,
	})
	if err != nil {
		return nil, err
	}
	metas, err := internal.GetBackupFileMetas(pinnedPaths)
	if err != nil {
		return nil, err
	}
	if err = concurrentUploader.UploadBackupFiles(metas); err != nil {
		return nil, fmt.Errorf("upload pinned ts files: %w", err)
	}
	if _, err = concurrentUploader.Finalize(); err != nil {
		return nil, fmt.Errorf("finalize ts upload: %w", err)
	}

	now := utility.TimeNowCrossPlatformLocal()
	backup := &archive.Backup{
		BackupName:      args.DataPrefix,
		StartLocalTime:  now,
		FinishLocalTime: now,
		Permanent:       args.Permanent,
		DataSize:        concurrentUploader.UncompressedSize,
		BackupSize:      concurrentUploader.CompressedSize,
		BackupType:      archive.TSBackupType,
		TSBackupID:      args.BackupID,
		TSBackupPath:    args.BackupPath,
		TSDataSize:      concurrentUploader.UncompressedSize,
		TSFileCount:     int64(len(pinnedPaths)),
		TSStartTime:     now,
		TSFinishTime:    now,
	}
	if err = concurrentUploader.UploadSentinel(ctx, backup, args.DataPrefix); err != nil {
		return nil, fmt.Errorf("upload ts sentinel: %w", err)
	}
	return backup, nil
}

type FetchArgs struct {
	Folder     storage.Folder
	DataPrefix string
	TargetDir  string
	SkipClean  bool
}

// Fetch restores a tiered-storage tree from the data prefix into TargetDir.
func Fetch(ctx context.Context, args FetchArgs) error {
	if args.SkipClean {
		return fmt.Errorf("--%s is not supported for tiered-storage restores", "skip-clean")
	}
	downloader := internal.NewCommonDirectoryDownloader(args.Folder, args.DataPrefix)
	if err := downloader.DownloadDirectory(ctx, args.TargetDir); err != nil {
		return fmt.Errorf("download ts backup %s: %w", args.DataPrefix, err)
	}
	return nil
}

// StandaloneDataPrefix returns the data and sentinel base for a standalone TS backup.
func StandaloneDataPrefix() string {
	return archive.GenerateNewTSBackupName()
}

// AttachedDataPrefix returns the nested TS data prefix for a main backup.
func AttachedDataPrefix(backupName string) string {
	return archive.AttachedTSDataPrefix(backupName)
}

// AttachedSentinelPath returns the expected nested TS sentinel path.
func AttachedSentinelPath(backupName string) string {
	return archive.AttachedTSSentinelName(backupName)
}
