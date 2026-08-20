package aof

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/databases/redis/pin"
	"github.com/wal-g/wal-g/internal/diskwatcher"
	"github.com/wal-g/wal-g/utility"
)

type BackupService struct {
	DiskWatcher             *diskwatcher.DiskWatcher
	concurrentUploader      *internal.ConcurrentUploader
	metaConstructor         internal.MetaConstructor
	backupFilesListProvider *BackupFilesListProvider
	filesPinner             *FilesPinner
}

func GenerateNewBackupName() string {
	return "aof_" + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

type FilesPinner = pin.FilesPinner

func NewFilesPinner(path string) *FilesPinner {
	return pin.NewFilesPinner(path)
}

func CreateBackupService(diskWatcher *diskwatcher.DiskWatcher, uploader *internal.ConcurrentUploader,
	metaConstructor internal.MetaConstructor, backupFilesListProvider *BackupFilesListProvider, filesPinner *FilesPinner,
) (*BackupService, error) {
	return &BackupService{
		DiskWatcher:             diskWatcher,
		concurrentUploader:      uploader,
		backupFilesListProvider: backupFilesListProvider,
		filesPinner:             filesPinner,
		metaConstructor:         metaConstructor,
	}, nil
}

type DoBackupArgs struct {
	BackupName    string
	Sharded       bool
	DeferSentinel bool
}

func (bs *BackupService) DoBackup(ctx context.Context, args DoBackupArgs) error {
	err := bs.metaConstructor.Init(ctx)
	if err != nil {
		return errors.Wrapf(err, "can not init meta provider")
	}

	backupFiles := bs.backupFilesListProvider.Get()

	pinnedBackupFiles, err := bs.filesPinner.Pin(backupFiles)
	defer bs.filesPinner.Unpin()
	if err != nil {
		return errors.Wrapf(err, "unable to prevent files list from removal")
	}

	backupMetas, err := internal.GetBackupFileMetas(pinnedBackupFiles)
	if err != nil {
		return err
	}

	uploadErrChan := make(chan error)
	go func() {
		defer close(uploadErrChan)
		err := bs.concurrentUploader.UploadBackupFiles(backupMetas)
		if err != nil {
			uploadErrChan <- errors.Wrapf(err, "unable to upload backup files")
			return
		}
		uploadErrChan <- nil
	}()

	bs.DiskWatcher.Start()
	defer bs.DiskWatcher.Stop()

	select {
	case err := <-uploadErrChan:
		if err != nil {
			return err
		}
	case <-bs.DiskWatcher.Signaling:
		return fmt.Errorf("disk is filled above limit, exiting")
	}

	_, err = bs.concurrentUploader.Finalize()
	if err != nil {
		return err
	}

	fillArgs := archive.FillSlotsForShardedArgs{
		BackupName: args.BackupName,
		Sharded:    args.Sharded,
		Uploader:   bs.concurrentUploader,
	}
	err = archive.FillSlotsForSharded(ctx, fillArgs)
	if err != nil {
		return err
	}

	return bs.Finalize(ctx, args.BackupName, args.DeferSentinel)
}

func (bs *BackupService) Finalize(ctx context.Context, backupName string, deferSentinel bool) error {
	if err := bs.metaConstructor.Finalize(ctx, backupName); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	backupSentinelInfo := bs.metaConstructor.MetaInfo()
	backup := backupSentinelInfo.(*archive.Backup)
	backup.BackupName = backupName
	backup.BackupSize = bs.concurrentUploader.CompressedSize
	backup.DataSize = bs.concurrentUploader.UncompressedSize
	if !deferSentinel {
		if err := bs.concurrentUploader.UploadSentinel(ctx, backupSentinelInfo, backupName); err != nil {
			return fmt.Errorf("can not upload sentinel: %+v", err)
		}
	}
	return nil
}
