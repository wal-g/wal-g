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

// UploadBackup compresses a stream and uploads it, and uploads meta info
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error {
	err := metaConstructor.Init()
	if err != nil {
		return fmt.Errorf("can not init meta provider: %+v", err)
	}

	dstPath, err := su.PushStream(context.Background(), stream)
	if err != nil {
		return fmt.Errorf("can not upload backup: %+v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("backup command failed: %+v", err)
	}

	return su.Finalize(metaConstructor, dstPath)
}

func (su *StorageUploader) Finalize(metaConstructor internal.MetaConstructor, dstPath string) error {
	if err := metaConstructor.Finalize(dstPath); err != nil {
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
	if err := internal.UploadSentinel(su, backupSentinelInfo, dstPath); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}
