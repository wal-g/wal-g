package internal

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

const (
	StreamPrefix           = "stream_"
	StreamBackupNameLength = 23 // len(StreamPrefix) + len(utility.BackupTimeFormat)
)

// TODO : unit tests
// PushStream compresses a stream and push it
func (uploader *RegularUploader) PushStream(ctx context.Context, stream io.Reader) (string, error) {
	return uploader.PushStreamWithName(ctx, stream, StreamPrefix+utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat))
}

// PushStreamWithName compresses a stream and uploads it under the supplied backup name.
func (uploader *RegularUploader) PushStreamWithName(ctx context.Context, stream io.Reader, backupName string) (string, error) {
	dstPath := GetStreamName(backupName, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(ctx, stream, dstPath)

	return backupName, err
}

// TODO : unit tests
// returns backup_prefix
// (Note: individual parition names are built by adding '_0000.br' or '_0000_0000.br' suffix)
func (uploader *SplitStreamUploader) PushStream(ctx context.Context, stream io.Reader) (string, error) {
	return uploader.PushStreamWithName(ctx, stream, StreamPrefix+utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat))
}

// PushStreamWithName splits, compresses, and uploads a stream under the supplied backup name.
func (uploader *SplitStreamUploader) PushStreamWithName(ctx context.Context, stream io.Reader, backupName string) (string, error) {
	// Upload Stream:
	errGroup, egCtx := errgroup.WithContext(ctx)
	var readers = splitmerge.SplitReader(egCtx, stream, uploader.partitions, uploader.blockSize)
	for partNumber := 0; partNumber < uploader.partitions; partNumber++ {
		reader := readers[partNumber]
		if uploader.maxFileSize != 0 {
			currentPartNumber := partNumber
			errGroup.Go(func() error {
				idx := 0
				for {
					fileReader := io.LimitReader(reader, int64(uploader.maxFileSize))
					var read atomic.Int64
					fileReader = utility.NewWithSizeReader(fileReader, &read)

					tracelog.DebugLogger.Printf("Get file reader %d of part %d\n", idx, currentPartNumber)
					dstPath := GetPartitionedSteamMultipartName(backupName, uploader.Compression().FileExtension(), currentPartNumber, idx)
					err := uploader.PushStreamToDestination(egCtx, fileReader, dstPath)
					if err != nil {
						return err
					}
					if read.Load() == 0 {
						err = uploader.Folder().DeleteObjects(egCtx, []storage.Object{storage.NewLocalObject(dstPath, time.Time{}, 0)})
						return err
					}
					idx++
				}
			})
		} else {
			dstPath := GetPartitionedStreamName(backupName, uploader.Compression().FileExtension(), partNumber)
			errGroup.Go(func() error {
				return uploader.PushStreamToDestination(egCtx, reader, dstPath)
			})
		}
	}

	// Wait for upload finished:
	if err := errGroup.Wait(); err != nil {
		tracelog.WarningLogger.Printf("Failed to upload part of backup: %v", err)
		return backupName, err
	}

	// Upload StreamMetadata
	meta := BackupStreamMetadata{
		Type:        SplitMergeStreamBackup,
		Partitions:  uint(uploader.partitions),
		BlockSize:   uint(uploader.blockSize),
		Compression: uploader.Compression().FileExtension(),
	}
	uploaderClone := uploader.Clone()
	uploaderClone.DisableSizeTracking() // don't count metadata.json in backup size
	err := UploadBackupStreamMetadata(ctx, uploader, meta, backupName)

	return backupName, err
}

// TODO : unit tests
// PushStreamToDestination compresses a stream and push it to specifyed destination
func (uploader *RegularUploader) PushStreamToDestination(ctx context.Context, stream io.Reader, dstPath string) error {
	if uploader.dataSize != nil {
		stream = utility.NewWithSizeReader(stream, uploader.dataSize)
	}
	compressed := CompressAndEncrypt(stream, uploader.Compressor, ConfigureCrypter())
	err := uploader.Upload(ctx, dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)

	return err
}

func GetStreamName(backupName string, extension string) string {
	return utility.AddFileExtension(utility.SanitizePath(path.Join(backupName, "stream")), extension)
}

func GetPartitionedStreamName(backupName string, extension string, partIdx int) string {
	return utility.AddFileExtension(
		fmt.Sprintf("%s_%04d", utility.SanitizePath(path.Join(backupName, "part")), partIdx), extension)
}

func GetPartitionedSteamMultipartName(backupName string, extension string, partIdx int, fileNumber int) string {
	return utility.AddFileExtension(
		fmt.Sprintf("%s_%04d_%04d", utility.SanitizePath(path.Join(backupName, "part")), partIdx, fileNumber), extension)
}
