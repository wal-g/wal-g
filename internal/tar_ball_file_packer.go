package internal

import (
	"archive/tar"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
)

type TarBallFilePacker struct {
	timeline         uint32
	deltaMap         PagedFileDeltaMap
	incrementFromLsn *uint64
	files            BundleFiles
}

func newTarBallFilePacker(deltaMap PagedFileDeltaMap, incrementFromLsn *uint64, files BundleFiles) *TarBallFilePacker {
	return &TarBallFilePacker{
		deltaMap:         deltaMap,
		incrementFromLsn: incrementFromLsn,
		files:            files,
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
func (p *TarBallFilePacker) PackFileIntoTar(cfi *ComposeFileInfo, tarBall TarBall) error {
	incrementBaseLsn := p.incrementFromLsn
	var fileReader io.ReadCloser
	if cfi.isIncremented {
		bitmap, err := p.getDeltaBitmapFor(cfi.path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			p.files.AddSkippedFile(cfi.header, cfi.fileInfo)
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "PackFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.path)
		}
		fileReader, cfi.header.Size, err = ReadIncrementalFile(cfi.path, cfi.fileInfo.Size(), *incrementBaseLsn, bitmap)
		if os.IsNotExist(err) { // File was deleted before opening
			// We should ignore file here as if it did not exist.
			return nil
		}
		switch err.(type) {
		case nil:
			fileReader = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReader, &ioextensions.ZeroReader{}),
					N: cfi.header.Size,
				},
				Closer: fileReader,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.header.Name)
			cfi.isIncremented = false
			fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
			if err != nil {
				return err
			}
		default:
			return errors.Wrapf(err, "PackFileIntoTar: failed reading incremental file '%s'\n", cfi.path)
		}
	} else {
		var err error
		fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
		if err != nil {
			return err
		}
	}
	defer utility.LoggedClose(fileReader, "")
	p.files.AddFile(cfi.header, cfi.fileInfo, cfi.isIncremented)
	packedFileSize, err := PackFileTo(tarBall, cfi.header, fileReader)
	if err != nil {
		return errors.Wrap(err, "PackFileIntoTar: operation failed")
	}

	if packedFileSize != cfi.header.Size {
		return newTarSizeError(packedFileSize, cfi.header.Size)
	}

	return nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string, fileReader io.ReadCloser) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "startReadingFile: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := NewDiskLimitReader(file)
	fileReader = &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: fileInfoHeader.Size,
		},
		Closer: file,
	}
	return fileReader, nil
}
