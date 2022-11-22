package internal

import (
	"fmt"
	"io"
	"os"
	"path"

	"golang.org/x/sync/errgroup"

	"github.com/wal-g/wal-g/internal/splitmerge"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	StreamPrefix           = "stream_"
	StreamBackupNameLength = 23 // len(StreamPrefix) + len(utility.BackupTimeFormat)
)

// TODO : unit tests
// PushStream compresses a stream and push it
func (uploader *Uploader) PushStream(stream io.Reader) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
	dstPath := GetStreamName(backupName, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(stream, dstPath)

	return backupName, err
}

// TODO : unit tests
// returns backup_prefix
// (Note: individual parition names are built by adding '_0000.br' suffix)
func (uploader *SplitStreamUploader) PushStream(stream io.Reader) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)

	// Upload Stream:
	var readers = splitmerge.SplitReader(stream, uploader.partitions, uploader.blockSize)

	errGroup := new(errgroup.Group)
	for partNumber := 0; partNumber < uploader.partitions; partNumber++ {
		reader := readers[partNumber]
		if uploader.maxFileSize != 0 {
			currentPartNumber := partNumber
			errGroup.Go(func() error {
				idx := 0
				for {
					fileReader, err := utility.NewEOFProtectorReader(reader)
					if err == io.EOF {
						return nil
					}
					fileReader = io.LimitReader(fileReader, int64(uploader.maxFileSize))

					tracelog.DebugLogger.Printf("Get file reader %d of part %d\n", idx, currentPartNumber)
					dstPath := GetPartitionedStreamFileNumberName(backupName, uploader.Compressor.FileExtension(), currentPartNumber, idx)
					err = uploader.PushStreamToDestination(fileReader, dstPath)
					if err != nil {
						return err
					}
					idx++
				}
			})
		} else {
			dstPath := GetPartitionedStreamName(backupName, uploader.Compressor.FileExtension(), partNumber)
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
		Compression: uploader.Compressor.FileExtension(),
	}
	uploaderClone := uploader.Clone()
	uploaderClone.DisableSizeTracking() // don't count metadata.json in backup size
	err := UploadBackupStreamMetadata(uploader, meta, backupName)

	return backupName, err
}

// TODO : unit tests
// PushStreamToDestination compresses a stream and push it to specifyed destination
func (uploader *Uploader) PushStreamToDestination(stream io.Reader, dstPath string) error {
	if uploader.dataSize != nil {
		stream = NewWithSizeReader(stream, uploader.dataSize)
	}
	compressed := CompressAndEncrypt(stream, uploader.Compressor, ConfigureCrypter())
	err := uploader.Upload(dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)

	return err
}

// FileIsPiped Check if file is piped
func FileIsPiped(stream *os.File) bool {
	stat, _ := stream.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

func GetStreamName(backupName string, extension string) string {
	return utility.SanitizePath(path.Join(backupName, "stream.")) + extension
}

func GetPartitionedStreamName(backupName string, extension string, partIdx int) string {
	return fmt.Sprintf("%s_%04d.%s", utility.SanitizePath(path.Join(backupName, "part")), partIdx, extension)
}

func GetPartitionedStreamFileNumberName(backupName string, extension string, partIdx int, fileNumber int) string {
	return fmt.Sprintf("%s_%04d_%04d.%s", utility.SanitizePath(path.Join(backupName, "part")),
		partIdx, fileNumber, extension)
}
