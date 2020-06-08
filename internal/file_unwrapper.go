package internal

import (
	"archive/tar"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"io"
	"os"
)

type FileUnwrapperType int

const (
	DefaultBackupFileUnwrapper FileUnwrapperType = iota + 1
	CatchupBackupFileUnwrapper
)

func NewFileUnwrapper(unwrapperType FileUnwrapperType, options *BackupFileOptions) IBackupFileUnwrapper {
	switch unwrapperType {
	case DefaultBackupFileUnwrapper:
		return &DefaultFileUnwrapper{BackupFileUnwrapper{options}}
	case CatchupBackupFileUnwrapper:
		return &CatchupFileUnwrapper{BackupFileUnwrapper{options}}
	default:
		return &DefaultFileUnwrapper{BackupFileUnwrapper{options}}
	}
}

type BackupFileOptions struct {
	isIncremented bool
	isPageFile    bool
}

type IBackupFileUnwrapper interface {
	UnwrapNewFile(reader io.Reader, header *tar.Header, file *os.File) error
	UnwrapExistingFile(reader io.Reader, header *tar.Header, file *os.File) error
}

type BackupFileUnwrapper struct {
	options *BackupFileOptions
}

// truncate local file and set reader offset to zero
func (u *BackupFileUnwrapper) clearLocalFile(file *os.File) error {
	err := file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, 0)
	return err
}

// write file from reader to local file
func (u *BackupFileUnwrapper) writeLocalFile(fileReader io.Reader, header *tar.Header, localFile *os.File) error {
	_, err := io.Copy(localFile, fileReader)
	if err != nil {
		err1 := localFile.Close()
		if err1 != nil {
			tracelog.ErrorLogger.Printf("Interpret: failed to close localFile '%s' because of error: %v",
				localFile.Name(), err1)
		}
		err1 = os.Remove(localFile.Name())
		if err1 != nil {
			tracelog.ErrorLogger.Fatalf("Interpret: failed to remove localFile '%s' because of error: %v",
				localFile.Name(), err1)
		}
		return errors.Wrap(err, "Interpret: copy failed")
	}

	mode := os.FileMode(header.Mode)
	if err = os.Chmod(localFile.Name(), mode); err != nil {
		return errors.Wrap(err, "Interpret: chmod failed")
	}

	err = localFile.Sync()
	return errors.Wrap(err, "Interpret: fsync failed")
}
