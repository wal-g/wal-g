package walparser

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type InvalidRecordBlockIDError struct {
	error
}

func NewInvalidRecordBlockIDError(blockID uint8) InvalidRecordBlockIDError {
	return InvalidRecordBlockIDError{errors.Errorf("invalid record blockId: %v", blockID)}
}

func (err InvalidRecordBlockIDError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type OutOfOrderBlockIDError struct {
	error
}

func NewOutOfOrderBlockIDError(actualBlockID int, expectedBlockID int) OutOfOrderBlockIDError {
	return OutOfOrderBlockIDError{
		errors.Errorf("out of order block id: %v, expected: %v",
			actualBlockID,
			expectedBlockID)}
}

func (err OutOfOrderBlockIDError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InconsistentBlockDataStateError struct {
	error
}

func NewInconsistentBlockDataStateError(hasData bool, dataLength uint16) InconsistentBlockDataStateError {
	return InconsistentBlockDataStateError{
		errors.Errorf("block state is inconsistent: hasData is: %v, while dataLength is: %v",
			hasData,
			dataLength)}
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
	return ContinuationNotFoundError{
		errors.New(
			"expected to find continuation of current xlog record, but found new records instead")}
}
