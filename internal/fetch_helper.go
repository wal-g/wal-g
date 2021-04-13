package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os/user"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
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
func DownloadFile(folder storage.Folder, filename, ext string, writeCloser io.WriteCloser) error {
	decompressor := compression.FindDecompressor(ext)
	if decompressor == nil {
		return fmt.Errorf("decompressor for extension '%s' was not found", ext)
	}
	archiveReader, exists, err := TryDownloadFile(folder, filename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("File '%s' does not exist.\n", filename)
	}

	err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
	if err != nil {
		return err
	}
	utility.LoggedClose(writeCloser, "")
	return nil
}

func TryDownloadFile(folder storage.Folder, path string) (walFileReader io.ReadCloser, exists bool, err error) {
	walFileReader, err = folder.ReadObject(path)
	if err == nil {
		exists = true
		return
	}
	if _, ok := errors.Cause(err).(storage.ObjectNotFoundError); ok {
		err = nil
	}
	return
}

// TODO : unit tests
func DecompressDecryptBytes(dst io.Writer, archiveReader io.ReadCloser, decompressor compression.Decompressor) error {
	crypter := ConfigureCrypter()
	if crypter != nil {
		reader, err := crypter.Decrypt(archiveReader)
		if err != nil {
			return err
		}
		archiveReader = ioextensions.ReadCascadeCloser{
			Reader: reader,
			Closer: archiveReader,
		}
	}

	err := decompressor.Decompress(dst, archiveReader)
	return err
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
		file, err := ioutil.ReadFile(cacheFilename)
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
		return ioutil.WriteFile(cacheFilename, marshal, 0644)
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
func DownloadAndDecompressStorageFile(folder storage.Folder, fileName string) (io.ReadCloser, error) {
	for _, decompressor := range putCachedDecompressorInFirstPlace(compression.Decompressors) {
		archiveReader, exists, err := TryDownloadFile(folder, fileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		_ = SetLastDecompressor(decompressor)
		reader, writer := io.Pipe()
		go func() {
			err = DecompressDecryptBytes(&EmptyWriteIgnorer{writer}, archiveReader, decompressor)
			_ = writer.CloseWithError(err)
		}()
		return reader, nil
	}
	return nil, newArchiveNonExistenceError(fileName)
}

// TODO : unit tests
// DownloadFileTo downloads a file and writes it to local file
func DownloadFileTo(folder storage.Folder, fileName string, dstPath string) error {
	reader, err := DownloadAndDecompressStorageFile(folder, fileName)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(reader, "")
	return ioextensions.CreateFileWith(dstPath, reader)
}
