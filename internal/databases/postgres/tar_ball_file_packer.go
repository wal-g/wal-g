package postgres

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wal-g/wal-g/internal"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/internal/parallel"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

type SkippedFileError struct {
	error
}

func newSkippedFileError(path string) SkippedFileError {
	return SkippedFileError{errors.Errorf("File is skipped from the current backup: %s\n", path)}
}

func (err SkippedFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type FileNotExistError struct {
	error
}

func newFileNotExistError(path string) FileNotExistError {
	return FileNotExistError{errors.Errorf(
		"%s does not exist, probably deleted during the backup creation\n", path)}
}

func (err FileNotExistError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
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

// TarBallFilePacker is used to pack bundle file into tarball.
type TarBallFilePacker struct {
	deltaMap         PagedFileDeltaMap
	incrementFromLsn *uint64
	files            parallel.BundleFiles
	options          TarBallFilePackerOptions
}

func newTarBallFilePacker(deltaMap PagedFileDeltaMap, incrementFromLsn *uint64, files parallel.BundleFiles,
	options TarBallFilePackerOptions) *TarBallFilePacker {
	return &TarBallFilePacker{
		deltaMap:         deltaMap,
		incrementFromLsn: incrementFromLsn,
		files:            files,
		options:          options,
	}
}

func (p *TarBallFilePacker) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if p.deltaMap == nil {
		return nil, nil
	}
	return p.deltaMap.GetDeltaBitmapFor(filePath)
}

func (p *TarBallFilePacker) UpdateDeltaMap(deltaMap PagedFileDeltaMap) {
	p.deltaMap = deltaMap
}

// TODO : unit tests
func (p *TarBallFilePacker) PackFileIntoTar(cfi *parallel.ComposeFileInfo, tarBall internal.TarBall) error {
	fileReadCloser, err := p.createFileReadCloser(cfi)
	if err != nil {
		switch err.(type) {
		case SkippedFileError:
			p.files.AddSkippedFile(cfi.Header, cfi.FileInfo)
			return nil
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

func (p *TarBallFilePacker) createFileReadCloser(cfi *parallel.ComposeFileInfo) (io.ReadCloser, error) {
	var fileReadCloser io.ReadCloser
	if cfi.IsIncremented {
		bitmap, err := p.getDeltaBitmapFor(cfi.Path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			return nil, newSkippedFileError(cfi.Path)
		} else if err != nil {
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.Path)
		}
		fileReadCloser, cfi.Header.Size, err = ReadIncrementalFile(cfi.Path, cfi.FileInfo.Size(), *p.incrementFromLsn, bitmap)
		if errors.Is(err, os.ErrNotExist) {
			return nil, newFileNotExistError(cfi.Path)
		}
		switch err.(type) {
		case nil:
			fileReadCloser = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReadCloser, &ioextensions.ZeroReader{}),
					N: cfi.Header.Size,
				},
				Closer: fileReadCloser,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.Header.Name)
			cfi.IsIncremented = false
			fileReadCloser, err = startReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed reading incremental file '%s'\n", cfi.Path)
		}
	} else {
		var err error
		fileReadCloser, err = startReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
		if err != nil {
			return nil, err
		}
	}
	return fileReadCloser, nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, newFileNotExistError(path)
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

func verifyFile(path string, fileInfo os.FileInfo, fileReader io.Reader, isIncremented bool) ([]uint32, error) {
	if !isPagedFile(fileInfo, path) {
		_, err := io.Copy(io.Discard, fileReader)
		return nil, err
	}

	if isIncremented {
		return VerifyPagedFileIncrement(path, fileInfo, fileReader)
	}
	return VerifyPagedFileBase(path, fileInfo, fileReader)
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
