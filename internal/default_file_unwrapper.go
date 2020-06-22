package internal

import (
	"archive/tar"
	"github.com/pkg/errors"
	"io"
	"os"
)

// DefaultFileUnwrapper is used for default (backup-push) backups
type DefaultFileUnwrapper struct {
	BackupFileUnwrapper
}

func (u *DefaultFileUnwrapper) UnwrapNewFile(reader io.Reader, header *tar.Header, file *os.File) error {
	if u.options.isIncremented {
		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}
		err = CreateFileFromIncrement(reader, NewReadWriterAtFrom(file, fileInfo))
		return errors.Wrapf(err, "Interpret: failed to create file from increment '%s'", file.Name())
	}

	return u.writeLocalFile(reader, header, file)
}

func (u *DefaultFileUnwrapper) UnwrapExistingFile(reader io.Reader, header *tar.Header, file *os.File) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if u.options.isIncremented {
		err := WritePagesFromIncrement(reader, NewReadWriterAtFrom(file, fileInfo), false)
		return errors.Wrapf(err, "Interpret: failed to write increment to file '%s'", file.Name())
	}

	if u.options.isPageFile {
		err := RestoreMissingPages(reader, NewReadWriterAtFrom(file, fileInfo))
		return errors.Wrapf(err, "Interpret: failed to restore pages for file '%s'", file.Name())
	}

	// skip the non-page file because newer version is already on the disk
	return nil
}
