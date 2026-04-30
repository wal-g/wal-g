//go:build !windows
// +build !windows

package fsutil

import (
	"errors"
	"io"
)

func isEOFError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF)
}
