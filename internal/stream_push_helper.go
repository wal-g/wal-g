package internal

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/wal-g/tracelog"
	"golang.org/x/sync/errgroup"

	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/utility"
)

const (
	StreamPrefix           = "stream_"
	StreamBackupNameLength = 23 // len(StreamPrefix) + len(utility.BackupTimeFormat)
)

// TODO : unit tests
// PushStream compresses a stream and push it
func (uploader *RegularUploader) PushStream(stream io.Reader) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
	dstPath := GetStreamName(backupName, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(stream, dstPath)

	return backupName, err
}

// TODO : unit tests
// returns backup_prefix
// (Note: individual parition names are built by adding '_0000.br' or '_0000_0000.br' suffix)
func (uploader *SplitStreamUploader) PushStream(stream io.Reader) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)

	// Upload Stream:
	errGroup, ctx := errgroup.WithContext(context.Background())
	var readers = splitmerge.SplitReader(ctx, stream, uploader.partitions, uploader.blockSize)
	for partNumber := 0; partNumber < uploader.partitions; partNumber++ {
		reader := readers[partNumber]
		if uploader.maxFileSize != 0 {
			currentPartNumber := partNumber
			errGroup.Go(func() error {
				idx := 0
				for {
					fileReader := io.LimitReader(reader, int64(uploader.maxFileSize))
					read := int64(0)
					fileReader = utility.NewWithSizeReader(fileReader, &read)

					tracelog.DebugLogger.Printf("Get file reader %d of part %d\n", idx, currentPartNumber)
					dstPath := GetPartitionedSteamMultipartName(backupName, uploader.Compression().FileExtension(), currentPartNumber, idx)
					err := uploader.PushStreamToDestination(fileReader, dstPath)
					if err != nil {
						return err
					}
					if read == 0 {
						err = uploader.Folder().DeleteObjects([]string{dstPath})
						return err
					}
					idx++
				}
			})
		} else {
			dstPath := GetPartitionedStreamName(backupName, uploader.Compression().FileExtension(), partNumber)
			errGroup.Go(func() error {
				return uploader.PushStreamToDestination(reader, dstPath)
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
	err := UploadBackupStreamMetadata(uploader, meta, backupName)

	return backupName, err
}

// TODO : unit tests
// PushStreamToDestination compresses a stream and push it to specifyed destination
func (uploader *RegularUploader) PushStreamToDestination(stream io.Reader, dstPath string) error {
	if uploader.dataSize != nil {
		stream = utility.NewWithSizeReader(stream, uploader.dataSize)
	}
	compressed := CompressAndEncrypt(stream, uploader.Compressor, ConfigureCrypter())
	err := uploader.Upload(dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)

	return err
}

func GetStreamName(backupName string, extension string) string {
	return utility.SanitizePath(path.Join(backupName, "stream.")) + extension
}

func GetPartitionedStreamName(backupName string, extension string, partIdx int) string {
	return fmt.Sprintf("%s_%04d.%s", utility.SanitizePath(path.Join(backupName, "part")), partIdx, extension)
}

func GetPartitionedSteamMultipartName(backupName string, extension string, partIdx int, fileNumber int) string {
	return fmt.Sprintf("%s_%04d_%04d.%s", utility.SanitizePath(path.Join(backupName, "part")),
		partIdx, fileNumber, extension)
}
