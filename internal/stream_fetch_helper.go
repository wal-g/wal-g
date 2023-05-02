package internal

import (
	"fmt"
	"io"
	"path"
	"time"

	"github.com/wal-g/wal-g/internal/ioextensions"
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
		archiveReader, exists, err := TryDownloadFile(NewFolderReader(backup.Folder), GetStreamName(backup.Name, decompressor.FileExtension()))
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
func DownloadAndDecompressSplittedStream(backup Backup, blockSize int, extension string,
	writeCloser io.WriteCloser, maxDownloadRetry int) error {
	defer utility.LoggedClose(writeCloser, "")

	decompressor := compression.FindDecompressor(extension)
	if decompressor == nil {
		return fmt.Errorf("decompressor for file type '%s' not found", extension)
	}

	files, err := GetPartitionedBackupFileNames(backup, decompressor)
	if err != nil {
		return err
	}

	errorsPerWorker := make([]chan error, 0)
	writers := splitmerge.MergeWriter(utility.EmptyWriteCloserIgnorer{WriteCloser: writeCloser}, len(files), blockSize)

	for i, partitionFiles := range files {
		errCh := make(chan error)
		errorsPerWorker = append(errorsPerWorker, errCh)
		writer := writers[i]

		go func(files []string) {
			defer close(errCh)
			for _, fileName := range files {
				err := downloadAndDecompressFile(backup, decompressor, fileName, writer, maxDownloadRetry)
				if err != nil {
					tracelog.ErrorLogger.PrintOnError(writer.Close())
					errCh <- err
					return
				}
			}
			errCh <- writer.Close()
		}(partitionFiles)
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

func downloadAndDecompressFile(backup Backup, decompressor compression.Decompressor,
	fileName string, writer io.WriteCloser, maxDownloadRetry int) error {
	getArchiveReader := func() (io.ReadCloser, error) {
		archiveReader, exists, err := TryDownloadFile(NewFolderReader(backup.Folder), fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to dowload file %v: %w", fileName, err)
		} else if !exists {
			return nil, io.EOF
		} else {
			tracelog.DebugLogger.Printf("Found file: %s", fileName)
			return archiveReader, nil
		}
	}

	var archiveReader io.ReadCloser
	if maxDownloadRetry > 1 {
		archiveReader = ioextensions.NewReaderWithRetry(getArchiveReader, maxDownloadRetry)
	} else {
		reader, err := getArchiveReader()
		if err != nil {
			return err
		}
		archiveReader = reader
	}
	decompressedReader, err := DecompressDecryptBytes(archiveReader, decompressor)
	if err != nil {
		return fmt.Errorf("failed to decompress/decrypt file %v: %w", fileName, err)
	}
	defer utility.LoggedClose(decompressedReader, "")
	_, err = utility.FastCopy(writer, decompressedReader)
	if err != nil {
		return fmt.Errorf("failed to decompress/decrypt/pipe file %v: %w", fileName, err)
	}
	return nil
}

func GetPartitionedBackupFileNames(backup Backup, decompressor compression.Decompressor) ([][]string, error) {
	// list all files in backup folder:
	files, _, err := backup.Folder.GetSubFolder(backup.Name).ListFolder()
	if err != nil {
		return nil, fmt.Errorf("cannot list files in backup folder '%s' due to: %w", backup.Folder.GetPath(), err)
	}

	// prepare lookup table:
	fileNames := make(map[string]bool)
	for _, file := range files {
		filePath := path.Join(backup.Name, file.GetName())
		fileNames[filePath] = true
	}

	result := make([][]string, 0)
	partIdx := 0
	for {
		nextPartitionFirstFile := GetPartitionedSteamMultipartName(backup.Name, decompressor.FileExtension(), partIdx, 0)
		nextPartitionWholeFile := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), partIdx)
		if fileNames[nextPartitionFirstFile] {
			result = append(result, make([]string, 1))
			result[partIdx][0] = nextPartitionFirstFile
			fileIdx := 1
			for {
				nextPartitionFile := GetPartitionedSteamMultipartName(backup.Name, decompressor.FileExtension(), partIdx, fileIdx)
				if fileNames[nextPartitionFile] {
					result[partIdx] = append(result[partIdx], nextPartitionFile)
					fileIdx++
				} else {
					break
				}
			}
		} else if fileNames[nextPartitionWholeFile] {
			result = append(result, make([]string, 1))
			result[partIdx][0] = nextPartitionWholeFile
		} else {
			break
		}
		partIdx++
	}

	return result, nil
}
