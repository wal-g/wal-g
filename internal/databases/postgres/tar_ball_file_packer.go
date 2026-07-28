package postgres

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/ncw/directio"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	pg_errors "github.com/wal-g/wal-g/internal/databases/postgres/errors"
	"github.com/wal-g/wal-g/internal/databases/postgres/orioledb"
	"github.com/wal-g/wal-g/internal/ioextensions"
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

// TarBallFilePackerImpl is used to pack bundle file into tarball.
type TarBallFilePackerImpl struct {
	deltaMap             PagedFileDeltaMap
	incrementFromLsn     *LSN
	files                internal.BundleFiles
	options              TarBallFilePackerOptions
	IncrementFromChkpNum *uint32
}

func NewTarBallFilePacker(deltaMap PagedFileDeltaMap, incrementFromLsn *LSN, files internal.BundleFiles,
	options TarBallFilePackerOptions) *TarBallFilePackerImpl {
	return &TarBallFilePackerImpl{
		deltaMap:         deltaMap,
		incrementFromLsn: incrementFromLsn,
		files:            files,
		options:          options,
	}
}

func (p *TarBallFilePackerImpl) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if p.deltaMap == nil {
		return nil, nil
	}
	return p.deltaMap.GetDeltaBitmapFor(filePath)
}

func (p *TarBallFilePackerImpl) UpdateDeltaMap(deltaMap PagedFileDeltaMap) {
	p.deltaMap = deltaMap
}

// TODO : unit tests
func (p *TarBallFilePackerImpl) PackFileIntoTar(ctx context.Context, cfi *internal.ComposeFileInfo, tarBall internal.TarBall) error {
	fileReadCloser, err := p.createFileReadCloser(ctx, cfi)
	if err != nil {
		switch err.(type) {
		case SkippedFileError:
			p.files.AddSkippedFile(cfi.Header, cfi.FileInfo)
			return nil
		case internal.FileNotExistError:
			// File was deleted before opening.
			// We should ignore file here as if it did not exist.
			tracelog.WarningLogger.Println(err)
			return nil
		default:
			return err
		}
	}
	errorGroup, _ := errgroup.WithContext(ctx)

	if p.options.verifyPageChecksums {
		var secondReadCloser io.ReadCloser
		// newTeeReadCloser is used to provide the fileReadCloser to two consumers:
		// fileReadCloser is needed for PackFileTo, secondReadCloser is for the page verification
		if viper.GetBool(conf.DirectIO) {
			fileReadCloser, secondReadCloser = newTeeReadCloserDirectIO(fileReadCloser)
		} else {
			fileReadCloser, secondReadCloser = newTeeReadCloser(fileReadCloser)
		}
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

func (p *TarBallFilePackerImpl) createFileReadCloser(ctx context.Context, cfi *internal.ComposeFileInfo) (io.ReadCloser, error) {
	var fileReadCloser io.ReadCloser
	if cfi.IsIncremented {
		bitmap, err := p.getDeltaBitmapFor(cfi.Path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			return nil, newSkippedFileError(cfi.Path)
		} else if err != nil {
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.Path)
		}
		if p.IncrementFromChkpNum != nil && orioledb.IsOrioledbDataFile(cfi.FileInfo, cfi.Path) {
			fileReadCloser, cfi.Header.Size, err =
				orioledb.ReadIncrementalFile(ctx, cfi.Path, cfi.FileInfo.Size(), *p.IncrementFromChkpNum, bitmap)
		} else {
			fileReadCloser, cfi.Header.Size, err = ReadIncrementalFile(ctx, cfi.Path, cfi.FileInfo.Size(), *p.incrementFromLsn, bitmap)
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, internal.NewFileNotExistError(cfi.Path)
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
		case pg_errors.InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.Header.Name)
			cfi.IsIncremented = false
			fileReadCloser, err = internal.StartReadingFile(ctx, cfi.Header, cfi.FileInfo, cfi.Path)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed reading incremental file '%s'\n", cfi.Path)
		}
	} else {
		var err error
		fileReadCloser, err = internal.StartReadingFile(ctx, cfi.Header, cfi.FileInfo, cfi.Path)
		if err != nil {
			return nil, err
		}
	}
	return fileReadCloser, nil
}

func verifyFile(path string, fileInfo os.FileInfo, fileReader io.Reader, isIncremented bool) ([]uint32, error) {
	if !isChecksumValidatableFile(fileInfo, path) {
		tracelog.DebugLogger.Printf(
			"verifyFile: %s does not meet the criteria for checksum validation. "+
				"File will be copied without checksum verification.\n",
			path)
		_, err := io.Copy(io.Discard, fileReader)
		return nil, err
	}

	// if files don’t meet the size standard. The standard is that the file size divided by the block size should be an integer
	// then skip the block check and copy the file directly with a warning message
	if fileInfo.Size()%DatabasePageSize != 0 {
		tracelog.WarningLogger.Printf(
			"verifyFile: %s invalid file size %d. File copied without validation. "+
				"The file may be corrupted.\n",
			path, fileInfo.Size())
		_, err := io.Copy(io.Discard, fileReader)
		return nil, err
	}

	if isIncremented {
		return VerifyPagedFileIncrement(path, fileInfo, fileReader)
	}
	return VerifyPagedFileBase(path, fileInfo, fileReader)
}

// newTeeReadCloser creates two io.ReadClosers from one. Writes to the second consumer
// (page verification) are streamed directly through an unbuffered pipe.
func newTeeReadCloser(readCloser io.ReadCloser) (io.ReadCloser, io.ReadCloser) {
	pipeReader, pipeWriter := io.Pipe()

	// teeReader is used to provide the readCloser to two consumers
	teeReader := io.TeeReader(readCloser, pipeWriter)
	// MultiCloser closes both pipeWriter and readCloser on Close() call
	closer := ioextensions.NewMultiCloser([]io.Closer{readCloser, pipeWriter})
	return &ioextensions.ReadCascadeCloser{Reader: teeReader, Closer: closer}, pipeReader
}

// newTeeReadCloserDirectIO is newTeeReadCloser for the DIRECT_IO read path: it buffers writes to
// the second consumer (page verification) into WALG_DIRECT_IO_BLOCK_COUNT-sized blocks so the
// verifier is woken once per O_DIRECT block instead of once per io.Copy chunk.
func newTeeReadCloserDirectIO(readCloser io.ReadCloser) (io.ReadCloser, io.ReadCloser) {
	pipeReader, pipeWriter := io.Pipe()

	blockSize := viper.GetInt(conf.DirectIOBlockCountSetting) * directio.BlockSize
	bufferedWriter := bufio.NewWriterSize(pipeWriter, blockSize)
	teeReader := io.TeeReader(readCloser, bufferedWriter)

	// Flush the bufferedWriter before the pipe writer is closed so the final
	// partial block reaches the verifier.
	closer := ioextensions.NewMultiCloser([]io.Closer{
		readCloser,
		closerFunc(func() error {
			if err := bufferedWriter.Flush(); err != nil {
				return err
			}
			return pipeWriter.Close()
		}),
	})
	return &ioextensions.ReadCascadeCloser{Reader: teeReader, Closer: closer}, pipeReader
}

// closerFunc adapts a function to io.Closer.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }
