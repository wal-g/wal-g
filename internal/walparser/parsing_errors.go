package walparser

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
)

type InvalidRecordBlockIdError struct {
	error
}

func NewInvalidRecordBlockIdError(blockId uint8) InvalidRecordBlockIdError {
	return InvalidRecordBlockIdError{errors.Errorf("invalid record blockId: %v", blockId)}
}

func (err InvalidRecordBlockIdError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type OutOfOrderBlockIdError struct {
	error
}

func NewOutOfOrderBlockIdError(actualBlockId int, expectedBlockId int) OutOfOrderBlockIdError {
	return OutOfOrderBlockIdError{errors.Errorf("out of order block id: %v, expected: %v", actualBlockId, expectedBlockId)}
}

func (err OutOfOrderBlockIdError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InconsistentBlockDataStateError struct {
	error
}

func NewInconsistentBlockDataStateError(hasData bool, dataLength uint16) InconsistentBlockDataStateError {
	return InconsistentBlockDataStateError{errors.Errorf("block state is inconsistent: hasData is: %v, while dataLength is: %v", hasData, dataLength)}
}

func (err InconsistentBlockDataStateError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type NoPrevRelFileNodeError struct {
	error
}

func NewNoPrevRelFileNodeError() NoPrevRelFileNodeError {
	return NoPrevRelFileNodeError{errors.New("expected to copy previous rel file node, but not found one")}
}

func (err NoPrevRelFileNodeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type ContinuationNotFoundError struct {
	error
}

func NewContinuationNotFoundError() ContinuationNotFoundError {
	return ContinuationNotFoundError{errors.New("expected to find continuation of current xlog record, but found new records instead")}
}
