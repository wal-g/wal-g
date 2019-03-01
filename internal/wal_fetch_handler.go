package internal

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"os"
	"path"
	"time"
)

type InvalidWalFileMagicError struct {
	error
}

func NewInvalidWalFileMagicError() InvalidWalFileMagicError {
	return InvalidWalFileMagicError{errors.New("WAL-G: WAL file magic is invalid ")}
}

func (err InvalidWalFileMagicError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type ArchiveNonExistenceError struct {
	error
}

func NewArchiveNonExistenceError(archiveName string) ArchiveNonExistenceError {
	return ArchiveNonExistenceError{errors.Errorf("Archive '%s' does not exist.\n", archiveName)}
}

func (err ArchiveNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(folder storage.Folder, walFileName string, location string, triggerPrefetch bool) {
	tracelog.DebugLogger.Printf("HandleWALFetch(folder, %s, %s, %v)\n", walFileName, location, triggerPrefetch)
	folder = folder.GetSubFolder(WalPath)
	location = ResolveSymlink(location)
	if triggerPrefetch {
		defer forkPrefetch(walFileName, location)
	}

	_, _, running, prefetched := GetPrefetchLocations(path.Dir(location), walFileName)
	seenSize := int64(-1)

	for {
		if stat, err := os.Stat(prefetched); err == nil {
			if stat.Size() != int64(WalSegmentSize) {
				tracelog.ErrorLogger.Println("WAL-G: Prefetch error: wrong file size of prefetched file ", stat.Size())
				break
			}

			err = os.Rename(prefetched, location)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}

			err := checkWALFileMagic(location)
			if err != nil {
				tracelog.ErrorLogger.Println("Prefetched file contain errors", err)
				os.Remove(location)
				break
			}

			return
		} else if !os.IsNotExist(err) {
			tracelog.ErrorLogger.FatalError(err)
		}

		// We have race condition here, if running is renamed here, but it's OK

		if runStat, err := os.Stat(running); err == nil {
			observedSize := runStat.Size() // If there is no progress in 50 ms - start downloading myself
			if observedSize <= seenSize {
				defer func() {
					os.Remove(running) // we try to clean up and ignore here any error
					os.Remove(prefetched)
				}()
				break
			}
			seenSize = observedSize
		} else if os.IsNotExist(err) {
			break // Normal startup path
		} else {
			break // Abnormal path. Permission denied etc. Yes, I know that previous 'else' can be eliminated.
		}
		time.Sleep(50 * time.Millisecond)
	}

	err := downloadWALFileTo(folder, walFileName, location)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
}

// TODO : unit tests
func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer file.Close()
	magic := make([]byte, 4)
	file.Read(magic)
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return NewInvalidWalFileMagicError()
	}

	return nil
}

func TryDownloadWALFile(folder storage.Folder, walPath string) (walFileReader io.ReadCloser, exists bool, err error) {
	walFileReader, err = folder.ReadObject(walPath)
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
func decompressWALFile(dst io.Writer, archiveReader io.ReadCloser, decompressor Decompressor) error {
	crypter := OpenPGPCrypter{}
	if crypter.IsUsed() {
		reader, err := crypter.Decrypt(archiveReader)
		if err != nil {
			return err
		}
		archiveReader = ReadCascadeCloser{reader, archiveReader}
	}

	err := decompressor.Decompress(dst, archiveReader)
	return err
}

// TODO : unit tests
func downloadAndDecompressWALFile(folder storage.Folder, walFileName string) (io.ReadCloser, error) {
	for _, decompressor := range Decompressors {
		archiveReader, exists, err := TryDownloadWALFile(folder, walFileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		reader, writer := io.Pipe()
		go func() {
			err = decompressWALFile(&EmptyWriteIgnorer{writer}, archiveReader, decompressor)
			writer.CloseWithError(err)
		}()
		return reader, nil
	}
	return nil, NewArchiveNonExistenceError(walFileName)
}

// TODO : unit tests
// downloadWALFileTo downloads a file and writes it to local file
func downloadWALFileTo(folder storage.Folder, walFileName string, dstPath string) error {
	reader, err := downloadAndDecompressWALFile(folder, walFileName)
	if err != nil {
		return err
	}
	defer reader.Close()
	return CreateFileWith(dstPath, reader)
}
