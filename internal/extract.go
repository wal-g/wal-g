package internal

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/semaphore"
)

var MinExtractRetryWait = time.Minute
var MaxExtractRetryWait = 5 * time.Minute

type NoFilesToExtractError struct {
	error
}

func newNoFilesToExtractError() NoFilesToExtractError {
	return NoFilesToExtractError{errors.New("ExtractAll: did not provide files to extract")}
}

func (err NoFilesToExtractError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// UnsupportedFileTypeError is used to signal file types
// that are unsupported by WAL-G.
type UnsupportedFileTypeError struct {
	error
}

func newUnsupportedFileTypeError(path string, fileFormat string) UnsupportedFileTypeError {
	return UnsupportedFileTypeError{errors.Errorf("WAL-G does not support the file format '%s' in '%s'", fileFormat, path)}
}

func (err UnsupportedFileTypeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TarInterpreter behaves differently
// for different file types.
type TarInterpreter interface {
	Interpret(reader io.Reader, header *tar.Header) error
}

type GroupTarInterpreter interface {
	TarInterpreter
	InterpretGroup(tarReader *tar.Reader) error
}

type DevNullWriter struct {
	io.WriteCloser
	statPrinter sync.Once
	totalBytes  int64
}

func (e *DevNullWriter) Write(p []byte) (int, error) {
	e.statPrinter.Do(func() {
		go func() {
			for {
				time.Sleep(1 * time.Second)
				tracelog.ErrorLogger.Printf("/dev/null size %d", e.totalBytes)
			}
		}()
	})
	e.totalBytes += int64(len(p))
	return len(p), nil
}

var _ io.Writer = &DevNullWriter{}

// TODO : unit tests
// Extract exactly one tar bundle.
func extractOneTar(tarInterpreter TarInterpreter, source io.Reader) error {
	tarReader := tar.NewReader(source)

	if groupInterpreter, ok := tarInterpreter.(GroupTarInterpreter); ok {
		return groupInterpreter.InterpretGroup(tarReader)
	}

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "extractOne: tar extract failed")
		}

		err = tarInterpreter.Interpret(tarReader, header)
		if err != nil {
			return errors.Wrap(err, "extractOne: Interpret failed")
		}
	}
	return nil
}

func extractNonTar(tarInterpreter TarInterpreter, source io.Reader, path string, fileType FileType, mode int) error {
	var typeFlag byte
	if fileType == RegularFileType {
		typeFlag = tar.TypeReg
	} else {
		typeFlag = tar.TypeDir
	}
	return tarInterpreter.Interpret(source, &tar.Header{
		Name:     path,
		Mode:     int64(mode),
		Typeflag: typeFlag,
	})
}

// DecryptAndDecompressTar decrypts file and checks its extension.
// If it's tar, a decompression is not needed.
// Otherwise it uses corresponding decompressor. If none found an error will be returned.
func DecryptAndDecompressTar(reader io.Reader, filePath string, crypter crypto.Crypter) (io.ReadCloser, error) {
	var err error

	if crypter != nil {
		reader, err = crypter.Decrypt(reader)
		if err != nil {
			return nil, errors.Wrap(err, "DecryptAndDecompressTar: decrypt failed")
		}
	}

	fileExtension := utility.GetFileExtension(filePath)
	if fileExtension == "tar" {
		return io.NopCloser(reader), nil
	}

	decompressor := compression.FindDecompressor(fileExtension)
	if decompressor == nil {
		return nil, newUnsupportedFileTypeError(filePath, fileExtension)
	}

	return decompressor.Decompress(reader)
}

// ExtractAll Handles all files passed in. Supports `.lzo`, `.lz4`, `.lzma`, and `.tar`.
// File type `.nop` is used for testing purposes. Each file is extracted
// in its own goroutine and ExtractAll will wait for all goroutines to finish.
// Retries unsuccessful attempts log2(MaxConcurrency) times, dividing concurrency by two each time.
func ExtractAll(tarInterpreter TarInterpreter, files []ReaderMaker) error {
	return ExtractAllWithSleeper(tarInterpreter, files, NewExponentialSleeper(MinExtractRetryWait, MaxExtractRetryWait))
}

func ExtractAllWithSleeper(tarInterpreter TarInterpreter, files []ReaderMaker, sleeper Sleeper) error {
	if len(files) == 0 {
		return newNoFilesToExtractError()
	}

	// Set maximum number of goroutines spun off by ExtractAll
	downloadingConcurrency, err := GetMaxDownloadConcurrency()
	if err != nil {
		return err
	}
	for currentRun := files; len(currentRun) > 0; {
		failed := tryExtractFiles(currentRun, tarInterpreter, downloadingConcurrency)
		if downloadingConcurrency > 1 {
			downloadingConcurrency /= 2
		} else if len(failed) == len(currentRun) {
			return errors.Errorf("failed to extract files:\n%s\n",
				strings.Join(readerMakersToFilePaths(failed), "\n"))
		}
		currentRun = failed
		if len(failed) > 0 {
			sleeper.Sleep()
		}
	}

	return nil
}

// Extract single file from backup
// If it is .tar file unpack it and store internal files (there will be .tar file if you work with wal-g backup)
// Otherwise store this file (there will be regular file if you work with pgbackrest backup)
func extractFile(tarInterpreter TarInterpreter, extractingReader io.Reader, fileClosure ReaderMaker) error {
	switch fileClosure.FileType() {
	case TarFileType:
		err := extractOneTar(tarInterpreter, extractingReader)
		if err == nil {
			err = readTrailingZeros(extractingReader)
		}
		return err
	case RegularFileType:
		filePath := utility.TrimFileExtension(fileClosure.Path())
		return extractNonTar(tarInterpreter, extractingReader, filePath, fileClosure.FileType(), fileClosure.Mode())
	default:
		tracelog.InfoLogger.Print()
		return errors.New("Unknown fileType " + string(fileClosure.FileType()))
	}
}

// TODO : unit tests
func tryExtractFiles(files []ReaderMaker,
	tarInterpreter TarInterpreter,
	downloadingConcurrency int) (failed []ReaderMaker) {
	downloadingContext := context.TODO()
	downloadingSemaphore := semaphore.NewWeighted(int64(downloadingConcurrency))
	crypter := ConfigureCrypter()
	isFailed := sync.Map{}

	for _, file := range files {
		err := downloadingSemaphore.Acquire(downloadingContext, 1)
		if err != nil {
			tracelog.ErrorLogger.Println(err)
			return files //Should never happen, but if we are asked to cancel - consider all files unfinished
		}
		fileClosure := file

		go func() {
			defer downloadingSemaphore.Release(1)

			readCloser, err := fileClosure.Reader()
			if err == nil {
				defer utility.LoggedClose(readCloser, "")

				filePath := fileClosure.Path()
				var extractingReader io.ReadCloser
				extractingReader, err = DecryptAndDecompressTar(readCloser, filePath, crypter)
				if err == nil {
					defer extractingReader.Close()

					err = extractFile(tarInterpreter, extractingReader, fileClosure)
					err = errors.Wrapf(err, "Extraction error in %s", filePath)
					tracelog.InfoLogger.Printf("Finished extraction of %s", filePath)
				}
			}

			if err != nil {
				isFailed.Store(fileClosure, true)
				tracelog.ErrorLogger.Println(err)
			}
		}()
	}

	err := downloadingSemaphore.Acquire(downloadingContext, int64(downloadingConcurrency))
	if err != nil {
		tracelog.ErrorLogger.Println(err)
		return files //Should never happen, but if we are asked to cancel - consider all files unfinished
	}

	isFailed.Range(func(failedFile, _ interface{}) bool {
		failed = append(failed, failedFile.(ReaderMaker))
		return true
	})
	return failed
}

func readTrailingZeros(r io.Reader) error {
	// on first iteration we read small chunk
	// in most cases we will return fast without memory allocation
	b := make([]byte, 1024)
	for {
		n, err := r.Read(b)
		if n > 0 {
			if !utility.AllZero(b[:n]) {
				return io.ErrClosedPipe
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(b) < utility.CompressedBlockMaxSize {
			// but if we found zeroes, allocate large buffer to speed up reading
			b = make([]byte, utility.CompressedBlockMaxSize)
		}
	}
}
