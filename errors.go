package walg

import (
	"fmt"
)

// Lz4Error is used to catch specific errors from Lz4PipeWriter
// when uploading to S3. Will not retry upload if this error
// occurs.
type Lz4Error struct {
	err error
}

func (e Lz4Error) Error() string {
	msg := fmt.Sprintf("%+v\n", e.err)
	return msg
}

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

// NoMatchAvailableError is used to signal no match found in string.
type NoMatchAvailableError struct {
	str string
}

func (e NoMatchAvailableError) Error() string {
	msg := fmt.Sprintf("No match found in '%s'\n", e.str)
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
