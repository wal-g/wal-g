package internal

import (
	"fmt"
	"github.com/klauspost/readahead"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

func ParseTS(endTSEnvVar string) (endTS *time.Time, err error) {
	endTSStr, ok := GetSetting(endTSEnvVar)
	if ok {
		t, err := time.Parse(time.RFC3339, endTSStr)
		if err != nil {
			return nil, err
		}
		endTS = &t
	}
	return endTS, nil
}

// TODO : unit tests
// GetLogsDstSettings reads from the environment variables fetch settings
func GetLogsDstSettings(operationLogsDstEnvVariable string) (dstFolder string, err error) {
	dstFolder, ok := GetSetting(operationLogsDstEnvVariable)
	if !ok {
		return dstFolder, NewUnsetRequiredSettingError(operationLogsDstEnvVariable)
	}
	return dstFolder, nil
}

// TODO : unit tests
// downloadAndDecompressStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressStream(backup Backup, writeCloser io.WriteCloser) error {
	defer utility.LoggedClose(writeCloser, "")

	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadFile(backup.Folder, GetStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return errors.Wrapf(err, "failed to dowload file")
		}
		if !exists {
			continue
		}

		tracelog.DebugLogger.Printf("Found file: %s.%s", backup.Name, decompressor.FileExtension())
		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return errors.Wrapf(err, "failed to decompress and decrypt file")
		}
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

// TODO : unit tests
// DownloadAndDecompressStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressSplittedStream(backup Backup, partitions int, blockSize int, extension string, writeCloser io.WriteCloser) error {
	defer utility.LoggedClose(writeCloser, "")

	decompressor := compression.GetDecompressor(extension)
	if decompressor == nil {
		return fmt.Errorf("decompressor for file type '%s' not found", extension)
	}

	allErrors := make([]chan error, 0)
	writers, done := storage.MergeWriter(EmptyWriteIgnorer{WriteCloser: writeCloser}, partitions, blockSize)

	for i := 0; i < partitions; i++ {
		fileName := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), i)
		errCh := make(chan error)
		allErrors = append(allErrors, errCh)

		go func(fileName string, errCh chan error, writer io.WriteCloser) {
			archiveReader, exists, err := TryDownloadFile(backup.Folder, fileName)
			if err != nil {
				writer.Close()
				errCh <- errors.Wrapf(err, "failed to dowload file")
				return
			}
			if !exists {
				errCh <- writer.Close()
				return
			}

			tracelog.DebugLogger.Printf("Found files: %s", fileName)

			decryptReadCloser, err := DecryptBytes(archiveReader)
			if err != nil {
				errCh <- fmt.Errorf("failed to decrypt file: %v", err)
				return
			}

			asyncDecryptReadCloser := readahead.NewReadCloser(decryptReadCloser)

			err = decompressor.Decompress(writer, asyncDecryptReadCloser)
			if err != nil {
				errCh <- fmt.Errorf("failed to decompress archive reader: %w", err)
				return
			}
			err = writer.Close()
			errCh <- err
		}(fileName, errCh, writers[i])
	}

	var lastErr error
	for _, ch := range allErrors {
		select {
		case err := <-ch:
			if err != nil {
				tracelog.ErrorLogger.Printf("%v", err)
				lastErr = err
			}
		case err := <-done:
			return err
		}
	}

	// wait
	<-done

	return lastErr
}
