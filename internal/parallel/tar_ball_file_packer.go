package parallel

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type FileNotExistError struct {
	error
}

func NewFileNotExistError(path string) FileNotExistError {
	return FileNotExistError{errors.Errorf(
		"%s does not exist, probably deleted during the backup creation\n", path)}
}

func (err FileNotExistError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type TarBallFilePacker interface {
	PackFileIntoTar(cfi *ComposeFileInfo, tarBall internal.TarBall) error
}

type TarBallFilePackerOptions struct {
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
}

func NewTarBallFilePackerOptions(verifyPageChecksums, storeAllCorruptBlocks bool) TarBallFilePackerOptions {
	return TarBallFilePackerOptions{
		verifyPageChecksums:   verifyPageChecksums,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
	}
}

type RegularTarBallFilePacker struct {
	files   BundleFiles
	options TarBallFilePackerOptions
}

func NewRegularTarBallFilePacker(files BundleFiles, options TarBallFilePackerOptions) *RegularTarBallFilePacker {
	return &RegularTarBallFilePacker{
		files:   files,
		options: options,
	}
}

func (p *RegularTarBallFilePacker) PackFileIntoTar(cfi *ComposeFileInfo, tarBall internal.TarBall) error {
	fileReadCloser, err := startReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		switch err.(type) {
		case FileNotExistError:
			// File was deleted before opening.
			// We should ignore file here as if it did not exist.
			tracelog.WarningLogger.Println(err)
			return nil
		default:
			return err
		}
	}
	errorGroup, _ := errgroup.WithContext(context.Background())

	if p.options.verifyPageChecksums {
		var secondReadCloser io.ReadCloser
		// newTeeReadCloser is used to provide the fileReadCloser to two consumers:
		// fileReadCloser is needed for PackFileTo, secondReadCloser is for the page verification
		fileReadCloser, secondReadCloser = newTeeReadCloser(fileReadCloser)
		errorGroup.Go(func() (err error) {
			corruptBlocks, err := verifyFile(cfi.Path, cfi.FileInfo, secondReadCloser, cfi.IsIncremented)
			if err != nil {
				return err
			}
			p.files.AddFileWithCorruptBlocks(cfi.Header, cfi.FileInfo, cfi.IsIncremented,
				corruptBlocks, p.options.storeAllCorruptBlocks)
			return nil
		})
	} else {
		p.files.AddFile(cfi.Header, cfi.FileInfo, cfi.IsIncremented)
	}

	errorGroup.Go(func() error {
		defer utility.LoggedClose(fileReadCloser, "")
		packedFileSize, err := internal.PackFileTo(tarBall, cfi.Header, fileReadCloser)
		if err != nil {
			return errors.Wrap(err, "PackFileIntoTar: operation failed")
		}
		if packedFileSize != cfi.Header.Size {
			return newTarSizeError(packedFileSize, cfi.Header.Size)
		}
		return nil
	})

	return errorGroup.Wait()
}

func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, NewFileNotExistError(path)
		}
		return nil, errors.Wrapf(err, "startReadingFile: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := limiters.NewDiskLimitReader(file)
	fileReader := &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: fileInfoHeader.Size,
		},
		Closer: file,
	}
	return fileReader, nil
}

// Move to verifier?
func verifyFile(path string, fileInfo os.FileInfo, fileReader io.Reader, isIncremented bool) ([]uint32, error) {
	_, err := io.Copy(io.Discard, fileReader)
	return nil, err
}

// TeeReadCloser creates two io.ReadClosers from one
func newTeeReadCloser(readCloser io.ReadCloser) (io.ReadCloser, io.ReadCloser) {
	pipeReader, pipeWriter := io.Pipe()

	// teeReader is used to provide the readCloser to two consumers
	teeReader := io.TeeReader(readCloser, pipeWriter)
	// MultiCloser closes both pipeWriter and readCloser on Close() call
	closer := ioextensions.NewMultiCloser([]io.Closer{readCloser, pipeWriter})
	return &ioextensions.ReadCascadeCloser{Reader: teeReader, Closer: closer}, pipeReader
}
