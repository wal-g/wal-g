package rdb

import (
	"context"
	"fmt"
	"io"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
)

type StorageUploader struct {
	internal.Uploader
}

// NewRedisStorageUploader builds redis uploader, that also push metadata
func NewRedisStorageUploader(upl internal.Uploader) *StorageUploader {
	return &StorageUploader{upl}
}

type UploadBackupArgs struct {
	Cmd             internal.ErrWaiter
	MetaConstructor internal.MetaConstructor
	Sharded         bool
	Stream          io.Reader
}

// UploadBackup compresses a stream and uploads it, and uploads meta info
func (su *StorageUploader) UploadBackup(ctx context.Context, args UploadBackupArgs) error {
	if err := args.MetaConstructor.Init(ctx); err != nil {
		return fmt.Errorf("can not init meta provider: %+v", err)
	}

	dstPath, err := su.PushStream(ctx, args.Stream)
	if err != nil {
		return fmt.Errorf("can not upload backup: %+v", err)
	}

	if err := args.Cmd.Wait(); err != nil {
		return fmt.Errorf("backup command failed: %+v", err)
	}

	fillArgs := archive.FillSlotsForShardedArgs{
		BackupName: dstPath,
		Sharded:    args.Sharded,
		Uploader:   su,
	}
	if err := archive.FillSlotsForSharded(ctx, fillArgs); err != nil {
		return err
	}

	return su.Finalize(ctx, args.MetaConstructor, dstPath)
}

func (su *StorageUploader) Finalize(ctx context.Context, metaConstructor internal.MetaConstructor, dstPath string) error {
	if err := metaConstructor.Finalize(ctx, dstPath); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	backupSentinelInfo := metaConstructor.MetaInfo()

	uploadedSize, uploadedErr := su.UploadedDataSize()
	rawSize, rawErr := su.RawDataSize()
	if uploadedErr != nil || rawErr != nil {
		return fmt.Errorf("can not calc backup size: %+v", rawErr)
	}

	backup := backupSentinelInfo.(*archive.Backup)
	backup.BackupSize = uploadedSize
	backup.BackupName = dstPath
	backup.DataSize = rawSize
	if err := internal.UploadSentinel(ctx, su, backupSentinelInfo, dstPath); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}
