package walg

import (
	"github.com/pkg/errors"
	"io"
	"fmt"
)

type NoSuchFileError struct {
	error
}

func NewNoSuchFileError(filename string) NoSuchFileError {
	return NoSuchFileError{errors.Errorf("No file found: %s", filename)}
}

func (err NoSuchFileError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

type DataFolder interface {
	// OpenReadonlyFile should return NoSuchFileError if it cannot find desired file
	OpenReadonlyFile(filename string) (io.ReadCloser, error)
	OpenWriteOnlyFile(filename string) (io.WriteCloser, error)
	CleanFolder() error
}
