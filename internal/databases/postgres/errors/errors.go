package errors

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/logging"
)

// InvalidBlockError indicates that file contain invalid page and cannot be archived incrementally
type InvalidBlockError struct {
	error
}

func NewInvalidBlockError(blockNo uint32) InvalidBlockError {
	return InvalidBlockError{errors.Errorf("block %d is invalid", blockNo)}
}

func (err InvalidBlockError) Error() string {
	return fmt.Sprintf(logging.GetErrorFormatter(), err.error)
}
