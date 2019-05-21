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
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/internal/utils"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/semaphore"
)

var MinExtractRetryWait = time.Minute
var MaxExtractRetryWait = 5 * time.Minute

type NoFilesToExtractError struct {
	error
}

func NewNoFilesToExtractError() NoFilesToExtractError {
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

func NewUnsupportedFileTypeError(path string, fileFormat string) UnsupportedFileTypeError {
	return UnsupportedFileTypeError{errors.Errorf("WAL-G does not support the file format '%s' in '%s'", fileFormat, path)}
}

func (err UnsupportedFileTypeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// EmptyWriteIgnorer handles 0 byte write in LZ4 package
// to stop pipe reader/writer from blocking.
type EmptyWriteIgnorer struct {
	io.WriteCloser
}

func (e EmptyWriteIgnorer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return e.WriteCloser.Write(p)
}

// TODO : unit tests
// Extract exactly one tar bundle.
func extractOne(tarInterpreter TarInterpreter, source io.Reader) error {
	tarReader := tar.NewReader(source)

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

// TODO : unit tests
// Ensures that file extension is valid. Any subsequent behavior
// depends on file type.
func DecryptAndDecompressTar(writer io.Writer, readerMaker ReaderMaker, crypter crypto.Crypter) error {
	readCloser, err := readerMaker.Reader()

	if err != nil {
		return errors.Wrap(err, "DecryptAndDecompressTar: failed to create new reader")
	}
	defer readCloser.Close()

	if crypter != nil {
		var reader io.Reader
		reader, err = crypter.Decrypt(readCloser)
		if err != nil {
			return errors.Wrap(err, "DecryptAndDecompressTar: decrypt failed")
		}
		readCloser = utils.ReadCascadeCloser{
			Reader: reader,
			Closer: readCloser,
		}
	}

	fileExtension := utility.GetFileExtension(readerMaker.Path())
	for _, decompressor := range compression.Decompressors {
		if fileExtension != decompressor.FileExtension() {
			continue
		}
		err = decompressor.Decompress(writer, readCloser)
		return errors.Wrapf(err, "DecryptAndDecompressTar: %v decompress failed. Is archive encrypted?", decompressor.FileExtension())
	}
	switch fileExtension {
	case "tar":
		_, err = io.Copy(writer, readCloser)
		return errors.Wrap(err, "DecryptAndDecompressTar: tar extract failed")
	case "nop":
	case "lzo":
		return NewUnsupportedFileTypeError(readerMaker.Path(), fileExtension)
	default:
		return NewUnsupportedFileTypeError(readerMaker.Path(), fileExtension)
	}
	return nil
}

// TODO : unit tests
// ExtractAll Handles all files passed in. Supports `.lzo`, `.lz4`, `.lzma`, and `.tar`.
// File type `.nop` is used for testing purposes. Each file is extracted
// in its own goroutine and ExtractAll will wait for all goroutines to finish.
// Returns the first error encountered.
func ExtractAll(tarInterpreter TarInterpreter, files []ReaderMaker) error {
	if len(files) == 0 {
		return NewNoFilesToExtractError()
	}

	retrier := NewExponentialRetrier(MinExtractRetryWait, MaxExtractRetryWait)
	// Set maximum number of goroutines spun off by ExtractAll
	downloadingConcurrency, err := utility.GetMaxDownloadConcurrency(utility.Min(len(files), 10))
	if err != nil {
		return err
	}
	for currentRun := files; len(currentRun) > 0; {
		var failed []ReaderMaker
		failed = tryExtractFiles(currentRun, tarInterpreter, downloadingConcurrency)
		if downloadingConcurrency > 1 {
			downloadingConcurrency /= 2
		} else if len(failed) == len(currentRun) {
			return errors.Errorf("failed to extract files:\n%s\n",
				strings.Join(ReaderMakersToFilePaths(failed), "\n"))
		}
		currentRun = failed
		if len(failed) > 0 {
			retrier.retry()
		}
	}
	return nil
}

// TODO : unit tests
func tryExtractFiles(files []ReaderMaker, tarInterpreter TarInterpreter, downloadingConcurrency int) (failed []ReaderMaker) {
	downloadingContext := context.TODO()
	downloadingSemaphore := semaphore.NewWeighted(int64(downloadingConcurrency))
	crypter := openpgp.NewCrypter()
	isFailed := sync.Map{}

	for _, file := range files {
		downloadingSemaphore.Acquire(downloadingContext, 1)
		fileClosure := file

		extractingReader, pipeWriter := io.Pipe()
		decompressingWriter := &EmptyWriteIgnorer{pipeWriter}
		go func() {
			err := DecryptAndDecompressTar(decompressingWriter, fileClosure, crypter)
			decompressingWriter.Close()
			tracelog.InfoLogger.Printf("Finished decompression of %s", fileClosure.Path())
			if err != nil {
				isFailed.Store(fileClosure, true)
				tracelog.ErrorLogger.Println(fileClosure.Path(), err)
			}
		}()
		go func() {
			defer downloadingSemaphore.Release(1)
			err := extractOne(tarInterpreter, extractingReader)
			err = errors.Wrapf(err, "Extraction error in %s", fileClosure.Path())
			extractingReader.Close()
			tracelog.InfoLogger.Printf("Finished extraction of %s", fileClosure.Path())
			if err != nil {
				isFailed.Store(fileClosure, true)
				tracelog.ErrorLogger.Println(err)
			}
		}()
	}

	downloadingSemaphore.Acquire(downloadingContext, int64(downloadingConcurrency))
	isFailed.Range(func(failedFile, _ interface{}) bool {
		failed = append(failed, failedFile.(ReaderMaker))
		return true
	})
	return failed
}
