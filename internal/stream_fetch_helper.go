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
func DownloadAndDecompressSplittedStream(backup Backup, blockSize int, extension string, writeCloser io.WriteCloser, limitedFileSize bool) error {
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
	writers := splitmerge.MergeWriter(utility.EmptyWriteCloserIgnorer{WriteCloser: writeCloser}, partitions, blockSize)

	for i := 0; i < partitions; i++ {
		errCh := make(chan error)
		errorsPerWorker = append(errorsPerWorker, errCh)
		writer := writers[i]

		go func(idx int) {
			if limitedFileSize {
				errCh <- handleMultipleFilePartition(backup, decompressor, idx, writer)
			} else {
				errCh <- handleSingleFilePartition(backup, decompressor, idx, writer)
			}
		}(i)
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

func downloadFile(backup Backup, decompressor compression.Decompressor, fileName string, writer io.WriteCloser) error {
	archiveReader, exists, err := TryDownloadFile(backup.Folder, fileName)
	if err != nil {
		return fmt.Errorf("failed to dowload file %v: %w", fileName, err)
	}
	if !exists {
		return io.EOF
	}
	tracelog.DebugLogger.Printf("Found file: %s", fileName)
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

func handleSingleFilePartition(backup Backup, decompressor compression.Decompressor, fileIdx int, writer io.WriteCloser) error {
	fileName := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), fileIdx)
	tracelog.DebugLogger.Printf("Start decompress file: %s\n", fileName)
	err := downloadFile(backup, decompressor, fileName, writer)
	if err != nil {
		tracelog.ErrorLogger.PrintOnError(writer.Close())
		return err
	} else {
		return writer.Close()
	}
}

func handleMultipleFilePartition(backup Backup, decompressor compression.Decompressor, partIdx int, writer io.WriteCloser) error {
	defer utility.LoggedClose(writer, "")
	filesCount, err := detectFilesCount(backup, decompressor, partIdx)
	if err != nil {
		return err
	}

	fileNumber := 0
	for fileNumber < filesCount {
		fileName := GetPartitionedStreamFileNumberName(backup.Name, decompressor.FileExtension(), partIdx, fileNumber)
		err = downloadFile(backup, decompressor, fileName, writer)

		if err != nil {
			return err
		}

		fileNumber += 1
	}

	return nil
}

func getBackupFiles(backup Backup) (map[string]bool, error) {
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

	return fileNames, err
}

func detectFilesCount(backup Backup, decompressor compression.Decompressor, partIdx int) (int, error) {
	fileNames, err := getBackupFiles(backup)
	if err != nil {
		return -1, err
	}

	// find all backups:
	fileNumbers := 0
	for {
		fileName := GetPartitionedStreamFileNumberName(backup.Name, decompressor.FileExtension(), partIdx, fileNumbers)
		if fileNames[fileName] {
			fileNumbers++
			tracelog.DebugLogger.Printf("partition %s found in backup folder", fileName)
		} else {
			break
		}
	}

	if fileNumbers == 0 {
		return -1, fmt.Errorf("no backup files of part %d found in backup folder '%s'", partIdx, backup.Folder.GetPath())
	}

	return fileNumbers, nil
}

func detectPartitionsCount(backup Backup, decompressor compression.Decompressor) (int, error) {
	fileNames, err := getBackupFiles(backup)
	if err != nil {
		return -1, err
	}

	// find all backups:
	partitions := 0
	for {
		nextPartitionWholeFile := GetPartitionedStreamName(backup.Name, decompressor.FileExtension(), partitions)
		nextPartitionFirstFile := GetPartitionedStreamFileNumberName(backup.Name, decompressor.FileExtension(), partitions, 0)
		if fileNames[nextPartitionWholeFile] {
			partitions++
			tracelog.DebugLogger.Printf("partition %s found in backup folder", nextPartitionWholeFile)
		} else if fileNames[nextPartitionFirstFile] {
			partitions++
			tracelog.DebugLogger.Printf("partition %s found in backup folder", nextPartitionFirstFile)
		} else {
			break
		}
	}

	if partitions == 0 {
		return -1, fmt.Errorf("no backup partitions found in backup folder '%s'", backup.Folder.GetPath())
	}

	return partitions, nil
}
