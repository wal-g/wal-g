package walg

import (
	"fmt"
	"io"
)

type NoSuchFileError struct {
	filename string
}

func NewNoSuchFileError(filename string) *NoSuchFileError {
	return &NoSuchFileError{filename}
}

func (err NoSuchFileError) Error() string {
	return fmt.Sprintf("No file found: %s", err.filename)
}

type DataFolder interface {
	// OpenReadonlyFile should return NoSuchFileError if it cannot find desired file
	OpenReadonlyFile(filename string) (io.ReadCloser, error)
	OpenWriteOnlyFile(filename string) (io.WriteCloser, error)
	CleanFolder() error
}
