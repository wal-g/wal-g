package postgres

import (
	"archive/tar"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/utility"
)

// DefaultFileUnwrapper is used for default (backup-push) backups
type DefaultFileUnwrapper struct {
	BackupFileUnwrapper
}

func (u *DefaultFileUnwrapper) UnwrapNewFile(reader io.Reader, header *tar.Header,
	file *os.File, fsync bool) (*FileUnwrapResult, error) {
	if u.options.isIncremented {
		targetReadWriterAt, err := NewReadWriterAtFrom(file)
		if err != nil {
			return nil, err
		}
		missingBlockCount, err := CreateFileFromIncrement(reader, targetReadWriterAt)
		if err != nil {
			return nil, errors.Wrapf(err, "Interpret: failed to create file from increment '%s'", file.Name())
		}
		return NewCreatedFromIncrementResult(missingBlockCount), nil
	}
	err := utility.WriteLocalFile(reader, header, file, fsync)
	if err != nil {
		return nil, err
	}
	return NewCompletedResult(), nil
}

func (u *DefaultFileUnwrapper) UnwrapExistingFile(reader io.Reader, header *tar.Header,
	file *os.File, fsync bool) (*FileUnwrapResult, error) {
	targetReadWriterAt, err := NewReadWriterAtFrom(file)
	if err != nil {
		return nil, err
	}
	if u.options.isIncremented {
		restoredBlockCount, err := WritePagesFromIncrement(reader, targetReadWriterAt, false)
		if err != nil {
			return nil, errors.Wrapf(err, "Interpret: failed to write increment to file '%s'", file.Name())
		}
		return NewWroteIncrementBlocksResult(restoredBlockCount), nil
	}

	if u.options.isPageFile {
		err := RestoreMissingPages(reader, targetReadWriterAt)
		if err != nil {
			return nil, errors.Wrapf(err, "Interpret: failed to restore pages for file '%s'", file.Name())
		}
		return NewCompletedResult(), nil
	}

	// skip the non-page file because newer version is already on the disk
	return NewSkippedResult(), nil
}
