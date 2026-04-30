package fsutil

import (
	"errors"
	"io"

	"golang.org/x/sys/windows"
)

func isEOFError(err error) bool {
	// io.ReadFull seems to return ERROR_INVALID_PARAMETER instead of ErrUnexpectedEOF on windows when
	// the call is shorter than the buffer
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, windows.ERROR_INVALID_PARAMETER)
}
