package storage

import (
	"context"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

func IsTransientIOError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

type ObjectNotFoundError struct {
	error
}

func NewObjectNotFoundError(path string) ObjectNotFoundError {
	return ObjectNotFoundError{errors.Errorf("object '%s' not found in storage", path)}
}

func (err ObjectNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type Error struct {
	error
}

func NewError(err error, storageName, format string, args ...interface{}) Error {
	return Error{errors.Wrapf(err, storageName+" error : "+format, args...)}
}

func (err Error) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
