package walg

import (
	"archive/tar"
	"github.com/pkg/errors"
	"io"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"context"
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

// Extract exactly one tar bundle. Returns an error
// upon failure. Able to configure behavior by passing
// in different TarInterpreters.
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

// Ensures that file extension is valid. Any subsequent behavior
// depends on file type.
func decryptAndDecompressTar(writeCloser io.WriteCloser, readerMaker ReaderMaker, crypter Crypter) error {
	readCloser, err := readerMaker.Reader()

	if err != nil {
		return errors.Wrap(err, "ExtractAll: failed to create new reader")
	}
	defer readCloser.Close()

	if crypter.IsUsed() {
		var reader io.Reader
		reader, err = crypter.Decrypt(readCloser)
		if err != nil {
			return errors.Wrap(err, "ExtractAll: decrypt failed")
		}
		readCloser = ReadCascadeCloser{reader, readCloser}
	}

	for _, decompressor := range Decompressors {
		if readerMaker.Format() != decompressor.FileExtension() {
			continue
		}
		err = decompressor.Decompress(writeCloser, readCloser)
		if err != nil {
			return errors.Wrapf(err, "ExtractAll: %v decompress failed. Is archive encrypted?", decompressor.FileExtension())
		}
		return nil
	}
	switch readerMaker.Format() {
	case "tar":
		_, err = io.Copy(writeCloser, readCloser)
		if err != nil {
			return errors.Wrap(err, "ExtractAll: tar extract failed")
		}
	case "nop":
	case "lzo":
		return errors.Wrap(UnsupportedFileTypeError{readerMaker.Path(), readerMaker.Format()}, "ExtractAll: lzo linked to this WAL-G binary")
	default:
		return errors.Wrap(UnsupportedFileTypeError{readerMaker.Path(), readerMaker.Format()}, "ExtractAll:")
	}
	return nil
}

// ExtractAll Handles all files passed in. Supports `.lzo`, `.lz4`, `.lzma`, and `.tar`.
// File type `.nop` is used for testing purposes. Each file is extracted
// in its own goroutine and ExtractAll will wait for all goroutines to finish.
// Returns the first error encountered.
func ExtractAll(tarInterpreter TarInterpreter, files []ReaderMaker) error {
	if len(files) < 1 {
		return NoFilesToExtractError
	}

	var errorCollector errgroup.Group

	// Set maximum number of goroutines spun off by ExtractAll
	downloadingConcurrency := getMaxDownloadConcurrency(min(len(files), 10))
	downloadingContext := context.TODO()
	downloadingSemaphore := semaphore.NewWeighted(int64(downloadingConcurrency))
	var crypter OpenPGPCrypter

	for _, file := range files {
		downloadingSemaphore.Acquire(downloadingContext, 1)
		fileClosure := file

		extractingReader, pipeWriter := io.Pipe()
		decompressingWriter := &EmptyWriteIgnorer{pipeWriter}
		errorCollector.Go(func() error {
			err := decryptAndDecompressTar(decompressingWriter, fileClosure, &crypter)
			decompressingWriter.Close()
			return err
		})
		errorCollector.Go(func() error {
			defer downloadingSemaphore.Release(1)
			err := extractOne(tarInterpreter, extractingReader)
			extractingReader.Close()
			return err
		})
	}

	downloadingSemaphore.Acquire(downloadingContext, int64(downloadingConcurrency))
	return errorCollector.Wait()
}
