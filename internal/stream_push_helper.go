package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"io"
	"os"
	"path"

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
// SplitAndPushStream - splits stream to blocks of 1Mb, then puts it in `partitions` streams that are compressed
// and pushed to storage
// returns `;` separated list of files
func (uploader *Uploader) SplitAndPushStream(stream io.Reader, partitions int, blockSize int) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)

	var readers = storage.SplitReader(stream, partitions, blockSize)

	errCh := make(chan error)
	defer close(errCh)
	for i := 0; i < partitions; i++ {
		dstPath := GetPartitionedStreamName(backupName, uploader.Compressor.FileExtension(), i)
		tracelog.InfoLogger.Printf("Uploading... %v", dstPath)
		go func(reader io.Reader) {
			errCh <- uploader.PushStreamToDestination(reader, dstPath)
		}(readers[i])
	}

	// Wait for upload finished:
	var lastErr error
	for i := 0; i < partitions; i++ {
		err := <-errCh
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to upload part of backup: %v", err)
			lastErr = err
		}
	}

	return backupName, lastErr
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

func GetPartitionedStreamName(backupName string, extension string, parts int) string {
	return fmt.Sprintf("%s.%s_%04d", utility.SanitizePath(path.Join(backupName, "stream")), extension, parts)
}
