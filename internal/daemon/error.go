package daemon


import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type SocketWriteFailedError struct {
	error
}

func NewSocketWriteFailedError(socketError error) SocketWriteFailedError {
	return SocketWriteFailedError{errors.Errorf("socket write failed: %v", socketError)}
}

func (err SocketWriteFailedError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
