package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
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

func GetBackupStreamFetcher(backup Backup) (StreamFetcher, error) {
	var metadata BackupStreamMetadata
	err := backup.FetchDto(&metadata, StreamMetadataNameFromBackup(backup.Name))
	var test storage.ObjectNotFoundError
	if errors.As(err, &test) {
		return DownloadAndDecompressStream, nil
	}
	if err != nil {
		return nil, err
	}

	switch metadata.Type {
	case SplitMergeStreamBackup:
		var blockSize = metadata.BlockSize
		var compression = metadata.Compression
		return func(backup Backup, writer io.WriteCloser) error {
			return DownloadAndDecompressSplittedStream(backup, int(blockSize), compression, writer)
		}, nil
	case SingleStreamStreamBackup, "":
		return DownloadAndDecompressStream, nil
	}
	tracelog.ErrorLogger.Fatalf("Unknown backup type %s", metadata.Type)
	return nil, nil // unreachable
}

func UploadBackupStreamMetadata(uploader UploaderProvider, metadata interface{}, backupName string) error {
	sentinelName := StreamMetadataNameFromBackup(backupName)

	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return uploader.Upload(sentinelName, bytes.NewReader(raw))
}
