package wal_parser

import "fmt"

const (
	BkpImageHasHole uint8 = 0x01
	BkpImageIsCompressed uint8 = 0x02
	BkpImageApply uint8 = 0x04
)

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

type InconsistentBlockImageLengthError struct {
	hasHole bool
	isCompressed bool
	length uint16
}

func (err InconsistentBlockImageLengthError) Error() string {
	return fmt.Sprintf("block image has invalid state: hasHole: %v, isCompressed: %v, imageLength: %v", err.hasHole, err.isCompressed, err.length)
}

type XLogRecordBlockImageHeader struct {
	imageLength uint16
	holeOffset  uint16
	holeLength  uint16
	info        uint8
}

func (imageHeader *XLogRecordBlockImageHeader) hasHole() bool {
	return (imageHeader.info & BkpImageHasHole) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) isCompressed() bool {
	return (imageHeader.info & BkpImageIsCompressed) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) applyImage() bool {
	return (imageHeader.info & BkpImageApply) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) checkHoleStateConsistency() error {
	if (imageHeader.hasHole() && (imageHeader.holeOffset == 0 || imageHeader.holeLength == 0 || imageHeader.imageLength == BlockSize)) ||
		(!imageHeader.hasHole() && (imageHeader.holeOffset != 0 || imageHeader.holeLength != 0)) {
		return InconsistentBlockImageHoleStateError{imageHeader.holeOffset, imageHeader.holeLength,
			imageHeader.imageLength, imageHeader.hasHole()}
	}
	return nil
}

func (imageHeader *XLogRecordBlockImageHeader) checkLengthConsistency() error {
	if (imageHeader.isCompressed() && imageHeader.imageLength == BlockSize) ||
		(!imageHeader.hasHole() && !imageHeader.isCompressed() && imageHeader.imageLength != BlockSize){
		return InconsistentBlockImageLengthError{imageHeader.hasHole(), imageHeader.isCompressed(), imageHeader.imageLength}
	}
	return nil
}

func (imageHeader *XLogRecordBlockImageHeader) checkConsistency() error {
	err := imageHeader.checkHoleStateConsistency()
	if err != nil {
		return err
	}
	return imageHeader.checkLengthConsistency()
}
