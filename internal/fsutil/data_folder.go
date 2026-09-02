package fsutil

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type NoSuchFileError struct {
	error
}

func NewNoSuchFileError(filename string) NoSuchFileError {
	return NoSuchFileError{errors.Errorf("No file found: %s", filename)}
}

func (err NoSuchFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type DataFolder interface {
	// OpenReadonlyFile should return NoSuchFileError if it cannot find desired file
	OpenReadonlyFile(filename string) (io.ReadCloser, error)
	OpenWriteOnlyFile(filename string) (io.WriteCloser, error)
	CleanFolder() error
	FileExists(filename string) bool
	DeleteFile(filename string) error
	CreateFile(filename string) error
	RenameFile(oldFileName string, newFileName string) error
	// ListFiles returns the names of files in the folder, up to limit entries.
	// A limit <= 0 means no limit.
	ListFiles(limit int) ([]string, error)
}
