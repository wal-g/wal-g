package wal_parser

import "fmt"

type InvalidRecordBlockIdError struct {
	blockId uint8
}

func (err InvalidRecordBlockIdError) Error() string {
	return fmt.Sprintf("invalid record blockId: %v", err.blockId)
}

type OutOfOrderBlockIdError struct {
	actualBlockId   uint8
	expectedBlockId uint8
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

type InconsistentBlockImageHoleStateError struct {
	holeOffset uint16
	holeLength uint16
	imageLength uint16
	hasHole bool
}

func (err InconsistentBlockImageHoleStateError) Error() string {
	return fmt.Sprintf("block image hole state is inconsistent: holeOffset is: %v, holeLength is: %v, imageLength is: %v, while hasHole is: %v",
		err.holeOffset, err.holeLength, err.imageLength, err.hasHole)
}

type InvalidBlockImageStateError struct {
	hasHole bool
	isCompressed bool
	length uint16
}

func (err InvalidBlockImageStateError) Error() string {
	return fmt.Sprintf("block image has invalid state: hasHole: %v, isCompressed: %v, length: %v", err.hasHole, err.isCompressed, err.length)
}

type NoPrevRelFileNodeError struct {

}

func (err NoPrevRelFileNodeError) Error() string {
	return "expected to copy previous rel file node, but not found one"
}

type ContinuationNotFoundError struct {

}

func (err ContinuationNotFoundError) Error() string {
	return "expected to find continuation of current xlog record, but found new records instead"
}

