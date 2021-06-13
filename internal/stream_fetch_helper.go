package internal

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

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
func downloadAndDecompressStream(backup Backup, writeCloser io.WriteCloser) error {
	defer writeCloser.Close()

	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadFile(
			backup.Folder, GetStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(writeCloser, "")
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

func downloadAndDecompressStreamParts(backup Backup, writeCloser io.WriteCloser, fileNames []string) error {
	defer writeCloser.Close()
	decompressor := compression.FindDecompressor(filepath.Ext(fileNames[0]))
	if decompressor == nil {
		return newUnknownCompressionMethodError()
	}
	for _, fileName := range fileNames {
		archiveReader, exists, err := TryDownloadFile(
			backup.Folder, GetStreamName(fileName, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
		}
		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return err
		}
	}
	utility.LoggedClose(writeCloser, "")
	return nil
}
