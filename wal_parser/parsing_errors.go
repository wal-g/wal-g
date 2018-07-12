package wal_parser

import (
	"fmt"
	"errors"
)

type InvalidRecordBlockIdError struct {
	blockId uint8
}

func (err InvalidRecordBlockIdError) Error() string {
	return fmt.Sprintf("invalid record blockId: %v", err.blockId)
}

type OutOfOrderBlockIdError struct {
	actualBlockId   int
	expectedBlockId int
}

func (err OutOfOrderBlockIdError) Error() string {
	return fmt.Sprintf("out of order block id: %v, expected: %v", err.actualBlockId, err.expectedBlockId)
}

type InconsistentBlockDataStateError struct {
	hasData bool
	dataLength uint16
}

func (err InconsistentBlockDataStateError) Error() string {
	return fmt.Sprintf("block state is inconsistent: hasData is: %v, while dataLength is: %v", err.hasData, err.dataLength)
}

var NoPrevRelFileNodeError = errors.New("expected to copy previous rel file node, but not found one")
var ContinuationNotFoundError = errors.New("expected to find continuation of current xlog record, but found new records instead")
