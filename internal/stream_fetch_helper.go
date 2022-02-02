package internal

import (
	"fmt"
	"io"
	"path"
	"time"

	"github.com/wal-g/wal-g/internal/splitmerge"

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
			return fmt.Errorf("failed to dowload file: %w", err)
		}
		if !exists {
			continue
		}
		tracelog.DebugLogger.Printf("Found file: %s.%s", backup.Name, decompressor.FileExtension())
		defer utility.LoggedClose(archiveReader, "")

		decompressedReader, err := DecompressDecryptBytes(archiveReader, decompressor)
		if err != nil {
			return fmt.Errorf("failed to decompress and decrypt file: %w", err)
		}
		defer utility.LoggedClose(decompressedReader, "")

		_, err = utility.FastCopy(&utility.EmptyWriteIgnorer{Writer: writeCloser}, decompressedReader)
		if err != nil {
			return fmt.Errorf("failed to decompress and decrypt file: %w", err)
		}
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

// TODO : unit tests
// DownloadAndDecompressSplittedStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressSplittedStream(backup Backup, blockSize int, extension string, writeCloser io.WriteCloser) error {
	defer utility.LoggedClose(writeCloser, "")

	decompressor := compression.FindDecompressor(extension)
	if decompressor == nil {
		return fmt.Errorf("decompressor for file type '%s' not found", extension)
	}

	// detect number of partitions. For tiny backups it may not be equal to WALG_STREAM_SPLITTER_PARTITIONS
	partitions, err := detectPartitionsCount(backup, decompressor)
	if err != nil {
		return err
	}

	errorsPerWorker := make([]chan error, 0)
	writers := splitmerge.MergeWriter(utility.EmptyWriteIgnorer{Writer: writeCloser}, partitions, blockSize)

	for i := 0; i < partitions; i++ {
		fileName := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), i)
		errCh := make(chan error)
		errorsPerWorker = append(errorsPerWorker, errCh)

		go func(fileName string, errCh chan error, writer io.WriteCloser) {
			defer close(errCh)
			archiveReader, exists, err := TryDownloadFile(backup.Folder, fileName)
			if err != nil {
				tracelog.ErrorLogger.PrintOnError(writer.Close())
				errCh <- fmt.Errorf("failed to dowload file %v: %w", fileName, err)
				return
			}
			if !exists {
				errCh <- writer.Close()
				return
			}
			tracelog.DebugLogger.Printf("Found file: %s", fileName)
			decompressedReader, err := DecompressDecryptBytes(archiveReader, decompressor)
			if err != nil {
				tracelog.ErrorLogger.PrintOnError(writer.Close())
				errCh <- fmt.Errorf("failed to decompress/decrypt file %v: %w", fileName, err)
				return
			}
			defer utility.LoggedClose(decompressedReader, "")
			_, err = utility.FastCopy(writer, decompressedReader)
			if err != nil {
				tracelog.ErrorLogger.PrintOnError(writer.Close())
				errCh <- fmt.Errorf("failed to decompress/decrypt/pipe file %v: %w", fileName, err)
				return
			}
			errCh <- writer.Close()
		}(fileName, errCh, writers[i])
	}

	var lastErr error
	for _, ch := range errorsPerWorker {
		err := <-ch
		tracelog.ErrorLogger.PrintOnError(err)
		if (lastErr == nil && err != nil) || (lastErr == io.ErrShortWrite && err != io.ErrShortWrite) {
			lastErr = err
		}
	}

	return lastErr
}

func detectPartitionsCount(backup Backup, decompressor compression.Decompressor) (int, error) {
	// list all files in backup folder:
	files, _, err := backup.Folder.GetSubFolder(backup.Name).ListFolder()
	if err != nil {
		return -1, fmt.Errorf("cannot list files in backup folder '%s' due to: %w", backup.Folder.GetPath(), err)
	}

	// prepare lookup table:
	fileNames := make(map[string]bool)
	for _, file := range files {
		filePath := path.Join(backup.Name, file.GetName())
		fileNames[filePath] = true
	}

	// find all backups:
	partitions := 0
	for {
		nextPartition := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), partitions)
		if fileNames[nextPartition] {
			partitions++
			tracelog.DebugLogger.Printf("partition %s found in backup folder", nextPartition)
		} else {
			break
		}
	}

	if partitions == 0 {
		return -1, fmt.Errorf("no backup partitions found in backup folder '%s'", backup.Folder.GetPath())
	}

	return partitions, nil
}
