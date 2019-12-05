package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/tinsane/tracelog"
	"io"
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
	cleanFolder() error
	fileExists(filename string) bool
	deleteFile(filename string) error
	createFile(filename string) error
}
