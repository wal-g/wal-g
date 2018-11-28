package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// UnsetEnvVarError is used to indicate required environment
// variables for WAL-G.
type UnsetEnvVarError struct {
	error
}

func NewUnsetEnvVarError(names []string) UnsetEnvVarError {
	msg := "Did not set the following environment variables:\n"
	for _, v := range names {
		msg = msg + v + "\n"
	}
	return UnsetEnvVarError{errors.New(msg)}
}

func (err UnsetEnvVarError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// UnsupportedFileTypeError is used to signal file types
// that are unsupported by WAL-G.
type UnsupportedFileTypeError struct {
	error
}

func NewUnsupportedFileTypeError(path string, fileFormat string) UnsupportedFileTypeError {
	return UnsupportedFileTypeError{errors.Errorf("WAL-G does not support the file format '%s' in '%s'", fileFormat, path)}
}
