package walg

import (
	"archive/tar"
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"sync"
	"strings"
)

var NoFilesToExtractError = errors.New("ExtractAll: did not provide files to extract")

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
func extractOne(tarInterpreter TarInterpreter, src io.Reader) error {
	tarReader := tar.NewReader(src)

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
func DecryptAndDecompressTar(writer io.Writer, readerMaker ReaderMaker, crypter Crypter) error {
	readCloser, err := readerMaker.Reader()

	if err != nil {
		return errors.Wrap(err, "DecryptAndDecompressTar: failed to create new reader")
	}
	defer readCloser.Close()

	if crypter.IsUsed() {
		var reader io.Reader
		reader, err = crypter.Decrypt(readCloser)
		if err != nil {
			return errors.Wrap(err, "DecryptAndDecompressTar: decrypt failed")
		}
		readCloser = ReadCascadeCloser{reader, readCloser}
	}

	fileExtension := GetFileExtension(readerMaker.Path())
	for _, decompressor := range Decompressors {
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
		return errors.Wrap(UnsupportedFileTypeError{readerMaker.Path(), fileExtension}, "DecryptAndDecompressTar: lzo linked to this WAL-G binary")
	default:
		return errors.Wrap(UnsupportedFileTypeError{readerMaker.Path(), fileExtension}, "DecryptAndDecompressTar:")
	}
	return nil
}

// TODO : unit tests
// ExtractAll Handles all files passed in. Supports `.lzo`, `.lz4`, `.lzma`, and `.tar`.
// File type `.nop` is used for testing purposes. Each file is extracted
// in its own goroutine and ExtractAll will wait for all goroutines to finish.
// Returns the first error encountered.
func ExtractAll(tarInterpreter TarInterpreter, files []ReaderMaker) error {
	if len(files) < 1 {
		return NoFilesToExtractError
	}

	// Set maximum number of goroutines spun off by ExtractAll
	downloadingConcurrency := getMaxDownloadConcurrency(min(len(files), 10))
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
	}
	return nil
}

// TODO : unit tests
func tryExtractFiles(files []ReaderMaker, tarInterpreter TarInterpreter, downloadingConcurrency int) (failed []ReaderMaker) {
	downloadingContext := context.TODO()
	downloadingSemaphore := semaphore.NewWeighted(int64(downloadingConcurrency))
	var crypter OpenPGPCrypter
	inFailed := sync.Map{}

	for _, file := range files {
		downloadingSemaphore.Acquire(downloadingContext, 1)
		fileClosure := file

		extractingReader, pipeWriter := io.Pipe()
		decompressingWriter := &EmptyWriteIgnorer{pipeWriter}
		go func() {
			err := DecryptAndDecompressTar(decompressingWriter, fileClosure, &crypter)
			decompressingWriter.Close()
			log.Printf("Finished decompression of %s", fileClosure.Path())
			if err != nil {
				inFailed.Store(fileClosure, true)
				log.Println(err)
			}
		}()
		go func() {
			defer downloadingSemaphore.Release(1)
			err := extractOne(tarInterpreter, extractingReader)
			err = errors.Wrapf(err, "Extraction error in %s", fileClosure.Path())
			extractingReader.Close()
			log.Printf("Finished extraction of %s", fileClosure.Path())
			if err != nil {
				inFailed.Store(fileClosure, true)
				log.Println(err)
			}
		}()
	}

	downloadingSemaphore.Acquire(downloadingContext, int64(downloadingConcurrency))
	inFailed.Range(func(failedFile, _ interface{}) bool {
		failed = append(failed, failedFile.(ReaderMaker))
		return true
	})
	return failed
}
