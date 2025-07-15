package postgres

import (
	"archive/tar"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/utility"
)

// CatchupFileUnwrapper is used for catchup (catchup-push) backups
type CatchupFileUnwrapper struct {
	BackupFileUnwrapper
}

// truncate local file and set reader offset to zero
func clearLocalFile(file *os.File) error {
	err := file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, 0)
	return err
}

func (u *CatchupFileUnwrapper) UnwrapNewFile(reader io.Reader, header *tar.Header,
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

func (u *CatchupFileUnwrapper) UnwrapExistingFile(reader io.Reader, header *tar.Header,
	file *os.File, fsync bool) (*FileUnwrapResult, error) {
	if u.options.isIncremented {
		targetReadWriterAt, err := NewReadWriterAtFrom(file)
		if err != nil {
			return nil, err
		}
		restoredBlockCount, err := WritePagesFromIncrement(reader, targetReadWriterAt, true)
		if err != nil {
			return nil, errors.Wrapf(err, "Interpret: failed to write increment to file '%s'", file.Name())
		}
		return NewWroteIncrementBlocksResult(restoredBlockCount), nil
	}

	// clear the local file because there is a newer version for it
	err := clearLocalFile(file)
	if err != nil {
		return nil, err
	}
	err = utility.WriteLocalFile(reader, header, file, fsync)
	if err != nil {
		return nil, err
	}
	return NewCompletedResult(), nil
}
