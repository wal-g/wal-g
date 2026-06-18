package aof

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type RestoreService struct {
	SourceStorageFolder storage.Folder
	TargetDiskFolder    *archive.AofFolderInfo
	Uploader            internal.Uploader
	versionParser       *archive.VersionParser
}

type RestoreArgs struct {
	BackupName     string
	RestoreVersion string

	SkipChecks         bool
	SkipBackupDownload bool
}

func CreateRestoreService(sourceStorageFolder storage.Folder, targetDiskFolder *archive.AofFolderInfo,
	uploader internal.Uploader, versionParser *archive.VersionParser) (*RestoreService, error) {
	return &RestoreService{
		SourceStorageFolder: sourceStorageFolder,
		TargetDiskFolder:    targetDiskFolder,
		Uploader:            uploader,
		versionParser:       versionParser,
	}, nil
}

func (r *RestoreService) DoRestore(ctx context.Context, args RestoreArgs) error {
	sentinel, err := SentinelWithExistenceCheck(ctx, r.SourceStorageFolder, args.BackupName)
	if err != nil {
		return err
	}

	if !args.SkipChecks {
		ok, err := archive.EnsureRestoreCompatibility(sentinel.Version, args.RestoreVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("backup of version %s could not be restored to version %s", sentinel.Version, args.RestoreVersion)
		}

		err = archive.EnsureRedisStopped()
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped restore redis checks")
	}

	if !args.SkipBackupDownload {
		err = r.TargetDiskFolder.CleanPathAndParent()
		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("Download backup files to %s\n", r.TargetDiskFolder.Path)
		err = r.downloadFromTarArchives(ctx, sentinel.Name())
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped download redis backup files")
	}

	return nil
}

func (r *RestoreService) downloadFromTarArchives(ctx context.Context, backupName string) error {
	downloader := internal.CreateConcurrentDownloader(r.Uploader, nil)
	return downloader.Download(ctx, backupName, r.TargetDiskFolder.Path, nil)
}
