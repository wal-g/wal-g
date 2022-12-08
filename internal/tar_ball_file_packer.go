package internal

import (
	"archive/tar"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
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
	PackFileIntoTar(cfi *ComposeFileInfo, tarBall TarBall) error
}

type RegularTarBallFilePacker struct {
	files BundleFiles
}

func NewRegularTarBallFilePacker(files BundleFiles) *RegularTarBallFilePacker {
	return &RegularTarBallFilePacker{
		files: files,
	}
}

func (p *RegularTarBallFilePacker) PackFileIntoTar(cfi *ComposeFileInfo, tarBall TarBall) error {
	fileReadCloser, err := StartReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
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
	p.files.AddFile(cfi.Header, cfi.FileInfo, cfi.IsIncremented)

	defer utility.LoggedClose(fileReadCloser, "")
	packedFileSize, err := PackFileTo(tarBall, cfi.Header, fileReadCloser)
	if err != nil {
		return errors.Wrap(err, "PackFileIntoTar: operation failed")
	}
	if packedFileSize != cfi.Header.Size {
		return newTarSizeError(packedFileSize, cfi.Header.Size)
	}
	return nil
}

// TODO : unit tests
func StartReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string) (io.ReadSeekCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, NewFileNotExistError(path)
		}
		return nil, errors.Wrapf(err, "startReadingFile: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := limiters.NewDiskLimitReader(file)
	fileReader := &ioextensions.ReadSeekCloserImpl{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: fileInfoHeader.Size,
		},
		Closer: file,
		Seeker: file,
	}
	return fileReader, nil
}
