package walg

import (
	"fmt"
)

// UnsetEnvVarError is used to indicate required environment
// variables for WAL-G.
type UnsetEnvVarError struct {
	names []string
}

func (e UnsetEnvVarError) Error() string {
	msg := "Did not set the following environment variables:\n"
	for _, v := range e.names {
		msg = msg + v + "\n"
	}

	return msg
}

// UnsupportedFileTypeError is used to signal file types
// that are unsupported by WAL-G.
type UnsupportedFileTypeError struct {
	Path       string
	FileFormat string
}

func (e UnsupportedFileTypeError) Error() string {
	msg := fmt.Sprintf("WAL-G does not support the file format '%s' in '%s'", e.FileFormat, e.Path)
	return msg
}
