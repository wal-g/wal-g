package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type ArchiveNonExistenceError struct {
	error
}

func newArchiveNonExistenceError(archiveName string) ArchiveNonExistenceError {
	return ArchiveNonExistenceError{errors.Errorf("Archive '%s' does not exist.\n", archiveName)}
}

func (err ArchiveNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// DownloadFile downloads, decompresses and decrypts
func DownloadFile(reader StorageFolderReader, filename, ext string, writeCloser io.WriteCloser) error {
	utility.LoggedClose(writeCloser, "")

	decompressor := compression.FindDecompressor(ext)
	if decompressor == nil {
		return fmt.Errorf("decompressor for extension '%s' was not found", ext)
	}
	tracelog.DebugLogger.Printf("Found decompressor for %s", decompressor.FileExtension())

	archiveReader, exists, err := TryDownloadFile(reader, filename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("file '%s' does not exist", filename)
	}
	defer utility.LoggedClose(archiveReader, "")

	decompressedReader, err := DecompressDecryptBytes(archiveReader, decompressor)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(decompressedReader, "")

	_, err = utility.FastCopy(&utility.EmptyWriteIgnorer{Writer: writeCloser}, decompressedReader)
	return err
}

func TryDownloadFile(reader StorageFolderReader, path string) (fileReader io.ReadCloser, exists bool, err error) {
	fileReader, err = reader.ReadObject(path)
	if err == nil {
		exists = true
		return
	}
	if _, ok := errors.Cause(err).(storage.ObjectNotFoundError); ok {
		exists = true
		err = nil
	}
	return
}

func DecompressDecryptBytes(archiveReader io.Reader, decompressor compression.Decompressor) (io.ReadCloser, error) {
	decryptReader, err := DecryptBytes(archiveReader)
	if err != nil {
		return nil, err
	}
	if decompressor == nil {
		tracelog.DebugLogger.Printf("No decompressor has been selected")
		return io.NopCloser(decryptReader), nil
	}
	return decompressor.Decompress(decryptReader)
}

func DecryptBytes(archiveReader io.Reader) (io.Reader, error) {
	crypter := ConfigureCrypter()
	if crypter == nil {
		tracelog.DebugLogger.Printf("No crypter has been selected")
		return archiveReader, nil
	}

	tracelog.DebugLogger.Printf("Selected crypter: %s", crypter.Name())

	decryptReader, err := crypter.Decrypt(archiveReader)
	if err != nil {
		return nil, fmt.Errorf("failed to init decrypt reader: %w", err)
	}

	return decryptReader, nil
}

// CachedDecompressor is the file extension describing decompressor
type CachedDecompressor struct {
	FileExtension string
}

func GetLastDecompressor() (compression.Decompressor, error) {
	var cache CachedDecompressor
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, ".walg_decompressor_cache")
		file, err := os.ReadFile(cacheFilename)
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err != nil {
				return nil, err
			}
			return compression.FindDecompressor(cache.FileExtension), nil
		}
		return nil, err
	}

	return nil, nil
}

func SetLastDecompressor(decompressor compression.Decompressor) error {
	var cache CachedDecompressor
	usr, err := user.Current()

	if err != nil {
		return err
	}

	cacheFilename := filepath.Join(usr.HomeDir, ".walg_decompressor_cache")
	cache.FileExtension = decompressor.FileExtension()

	marshal, err := json.Marshal(&cache)
	if err == nil {
		return os.WriteFile(cacheFilename, marshal, 0644)
	}

	return err
}

func convertDecompressorList(decompressors []compression.Decompressor,
	lastDecompressor compression.Decompressor) []compression.Decompressor {
	ret := append(make([]compression.Decompressor, 0, len(decompressors)), lastDecompressor)

	for _, elem := range decompressors {
		if elem != lastDecompressor {
			ret = append(ret, elem)
		}
	}

	return ret
}

func putCachedDecompressorInFirstPlace(decompressors []compression.Decompressor) []compression.Decompressor {
	lastDecompressor, _ := GetLastDecompressor()

	if lastDecompressor != nil && lastDecompressor != decompressors[0] {
		return convertDecompressorList(decompressors, lastDecompressor)
	}

	return decompressors
}

// TODO : unit tests
func DownloadAndDecompressStorageFile(reader StorageFolderReader, fileName string) (io.ReadCloser, error) {
	archiveReader, decompressor, err := findDecompressorAndDownload(reader, fileName)
	if err != nil {
		return nil, err
	}

	decompressedReaded, err := DecompressDecryptBytes(archiveReader, decompressor)
	if err != nil {
		utility.LoggedClose(archiveReader, "")
		return nil, err
	}

	return ioextensions.ReadCascadeCloser{
		Reader: decompressedReaded,
		Closer: ioextensions.NewMultiCloser([]io.Closer{archiveReader, decompressedReaded}),
	}, nil
}

func findDecompressorAndDownload(reader StorageFolderReader, fileName string) (io.ReadCloser, compression.Decompressor, error) {
	for _, decompressor := range putCachedDecompressorInFirstPlace(compression.Decompressors) {
		archiveReader, exists, err := TryDownloadFile(reader, fileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			continue
		}
		_ = SetLastDecompressor(decompressor)

		return archiveReader, decompressor, nil
	}

	fileReader, exists, err := TryDownloadFile(reader, fileName)
	if err != nil {
		return nil, nil, err
	}
	if exists {
		return fileReader, nil, nil
	}

	return nil, nil, newArchiveNonExistenceError(fileName)
}

// TODO : unit tests
// DownloadFileTo downloads a file and writes it to local file
func DownloadFileTo(folderReader StorageFolderReader, fileName string, dstPath string) error {
	// Create file as soon as possible. It may be important due to race condition in wal-prefetch for PG.
	file, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")

	reader, err := DownloadAndDecompressStorageFile(folderReader, fileName)
	if err != nil {
		// We could not start upload - remove the file totally.
		_ = os.Remove(dstPath)
		return err
	}
	defer utility.LoggedClose(reader, "")

	_, err = utility.FastCopy(file, reader)
	// In case of error we may have some content within file. Leave it alone.
	return err
}
