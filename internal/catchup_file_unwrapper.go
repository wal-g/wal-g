package internal

import (
	"archive/tar"
	"io"
	"os"

	"github.com/pkg/errors"
)

// CatchupFileUnwrapper is used for catchup (catchup-push) backups
type CatchupFileUnwrapper struct {
	BackupFileUnwrapper
}

func (u *CatchupFileUnwrapper) UnwrapNewFile(reader io.Reader, header *tar.Header,
	file *os.File) (*FileUnwrapResult, error) {
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
	err := u.writeLocalFile(reader, header, file)
	if err != nil {
		return nil, err
	}
	return NewCompletedResult(), nil
}

func (u *CatchupFileUnwrapper) UnwrapExistingFile(reader io.Reader, header *tar.Header,
	file *os.File) (*FileUnwrapResult, error) {
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
	err := u.clearLocalFile(file)
	if err != nil {
		return nil, err
	}
	err = u.writeLocalFile(reader, header, file)
	if err != nil {
		return nil, err
	}
	return NewCompletedResult(), nil
}
