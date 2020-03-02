package walparser

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	BkpImageHasHole      uint8 = 0x01
	BkpImageIsCompressed uint8 = 0x02
	BkpImageApply        uint8 = 0x04
)

type InconsistentBlockImageHoleStateError struct {
	error
}

func NewInconsistentBlockImageHoleStateError(holeOffset uint16, holeLength uint16, imageLength uint16, hasHole bool) InconsistentBlockImageHoleStateError {
	return InconsistentBlockImageHoleStateError{errors.Errorf(
		"block image hole state is inconsistent: holeOffset is: %v, holeLength is: %v, imageLength is: %v, while hasHole is: %v",
		holeOffset, holeLength, imageLength, hasHole)}
}

func (err InconsistentBlockImageHoleStateError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InconsistentBlockImageLengthError struct {
	error
}

func NewInconsistentBlockImageLengthError(hasHole bool, isCompressed bool, length uint16) InconsistentBlockImageLengthError {
	return InconsistentBlockImageLengthError{errors.Errorf("block image has invalid state: hasHole: %v, isCompressed: %v, imageLength: %v", hasHole, isCompressed, length)}
}

func (err InconsistentBlockImageLengthError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type XLogRecordBlockImageHeader struct {
	ImageLength uint16
	HoleOffset  uint16
	HoleLength  uint16
	Info        uint8
}

func (imageHeader *XLogRecordBlockImageHeader) HasHole() bool {
	return (imageHeader.Info & BkpImageHasHole) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) IsCompressed() bool {
	return (imageHeader.Info & BkpImageIsCompressed) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) ApplyImage() bool {
	return (imageHeader.Info & BkpImageApply) != 0
}

func (imageHeader *XLogRecordBlockImageHeader) checkHoleStateConsistency() error {
	if (imageHeader.HasHole() && (imageHeader.HoleOffset == 0 || imageHeader.HoleLength == 0 || imageHeader.ImageLength == BlockSize)) ||
		(!imageHeader.HasHole() && (imageHeader.HoleOffset != 0 || imageHeader.HoleLength != 0)) {
		return NewInconsistentBlockImageHoleStateError(imageHeader.HoleOffset, imageHeader.HoleLength,
			imageHeader.ImageLength, imageHeader.HasHole())
	}
	return nil
}

func (imageHeader *XLogRecordBlockImageHeader) checkLengthConsistency() error {
	if (imageHeader.IsCompressed() && imageHeader.ImageLength == BlockSize) ||
		(!imageHeader.HasHole() && !imageHeader.IsCompressed() && imageHeader.ImageLength != BlockSize) {
		return NewInconsistentBlockImageLengthError(imageHeader.HasHole(), imageHeader.IsCompressed(), imageHeader.ImageLength)
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
