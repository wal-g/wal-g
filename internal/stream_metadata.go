package internal

import (
	"context"
	"errors"
	"io"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	SplitMergeStreamBackup   = "SPLIT_MERGE_STREAM_BACKUP"
	SingleStreamStreamBackup = "STREAM_BACKUP"
)

type BackupStreamMetadata struct {
	Type        string `json:"type"`
	Partitions  uint   `json:"partitions,omitempty"`
	BlockSize   uint   `json:"block_size,omitempty"`
	Compression string `json:"compression,omitempty"`
}

func GetBackupStreamFetcher(ctx context.Context, backup Backup) (StreamFetcher, error) {
	var metadata BackupStreamMetadata
	err := FetchDto(ctx, backup.Folder, &metadata, StreamMetadataNameFromBackup(backup.Name))
	var test storage.ObjectNotFoundError
	if errors.As(err, &test) {
		return DownloadAndDecompressStream, nil
	}
	if err != nil {
		return nil, err
	}
	maxDownloadRetry := viper.GetInt(conf.MysqlBackupDownloadMaxRetry)

	switch metadata.Type {
	case SplitMergeStreamBackup:
		var blockSize = metadata.BlockSize
		var compression = metadata.Compression
		return func(ctx context.Context, backup Backup, writer io.WriteCloser) error {
			return DownloadAndDecompressSplittedStream(ctx, backup, int(blockSize), compression, writer, maxDownloadRetry)
		}, nil
	case SingleStreamStreamBackup, "":
		return DownloadAndDecompressStream, nil
	}
	tracelog.ErrorLogger.Fatalf("Unknown backup type %s", metadata.Type)
	return nil, nil // unreachable
}

func UploadBackupStreamMetadata(ctx context.Context, uploader Uploader, metadata interface{}, backupName string) error {
	sentinelName := StreamMetadataNameFromBackup(backupName)
	return UploadDto(ctx, uploader.Folder(), metadata, sentinelName)
}
