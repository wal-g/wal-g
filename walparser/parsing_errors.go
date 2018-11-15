package walparser

import (
	"github.com/pkg/errors"
)

type InvalidRecordBlockIdError struct {
	error
}

func NewInvalidRecordBlockIdError(blockId uint8) InvalidRecordBlockIdError {
	return InvalidRecordBlockIdError{errors.Errorf("invalid record blockId: %v", blockId)}
}

type OutOfOrderBlockIdError struct {
	error
}

func NewOutOfOrderBlockIdError(actualBlockId int, expectedBlockId int) OutOfOrderBlockIdError {
	return OutOfOrderBlockIdError{errors.Errorf("out of order block id: %v, expected: %v", actualBlockId, expectedBlockId)}
}

type InconsistentBlockDataStateError struct {
	error
}

func NewInconsistentBlockDataStateError(hasData    bool, dataLength uint16) InconsistentBlockDataStateError {
	return InconsistentBlockDataStateError{errors.Errorf("block state is inconsistent: hasData is: %v, while dataLength is: %v", hasData, dataLength)}
}

type NoPrevRelFileNodeError struct {
	error
}

func NewNoPrevRelFileNodeError() NoPrevRelFileNodeError {
	return NoPrevRelFileNodeError{errors.New("expected to copy previous rel file node, but not found one")}
}

type ContinuationNotFoundError struct {
	error
}

func NewContinuationNotFoundError() ContinuationNotFoundError {
	return ContinuationNotFoundError{errors.New("expected to find continuation of current xlog record, but found new records instead")}
}
