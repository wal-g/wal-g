package aof

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
)

type RestoreService struct {
	Context       context.Context
	Folder        *archive.AofFolderInfo
	Uploader      internal.Uploader
	versionParser *archive.VersionParser
}

type RestoreArgs struct {
	BackupName     string
	RestoreVersion string

	SkipChecks         bool
	SkipBackupDownload bool
}

func CreateRestoreService(ctx context.Context, folder *archive.AofFolderInfo, uploader internal.Uploader,
	versionParser *archive.VersionParser) (*RestoreService, error) {
	return &RestoreService{
		Context:       ctx,
		Folder:        folder,
		Uploader:      uploader,
		versionParser: versionParser,
	}, nil
}

func (restoreService *RestoreService) DoRestore(args RestoreArgs) error {
	sentinel, err := DownloadSentinel(restoreService.Uploader.Folder(), args.BackupName)
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
		err = restoreService.Folder.CleanData()
		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("Download backup files to %s\n", restoreService.Folder.Path)
		err = restoreService.downloadFromTarArchives(args.BackupName)
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped download redis backup files")
	}

	return nil
}

func (restoreService *RestoreService) downloadFromTarArchives(backupName string) error {
	downloader := internal.CreateConcurrentDownloader(restoreService.Uploader)
	return downloader.Download(backupName, restoreService.Folder.Path)
}
