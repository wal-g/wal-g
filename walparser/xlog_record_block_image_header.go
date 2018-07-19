package walparser

import "fmt"

const (
	BkpImageHasHole      uint8 = 0x01
	BkpImageIsCompressed uint8 = 0x02
	BkpImageApply        uint8 = 0x04
)

type InconsistentBlockImageHoleStateError struct {
	holeOffset  uint16
	holeLength  uint16
	imageLength uint16
	hasHole     bool
}

func (err InconsistentBlockImageHoleStateError) Error() string {
	return fmt.Sprintf("block image hole state is inconsistent: holeOffset is: %v, holeLength is: %v, imageLength is: %v, while hasHole is: %v",
		err.holeOffset, err.holeLength, err.imageLength, err.hasHole)
}

type InconsistentBlockImageLengthError struct {
	hasHole      bool
	isCompressed bool
	length       uint16
}

func (err InconsistentBlockImageLengthError) Error() string {
	return fmt.Sprintf("block image has invalid state: hasHole: %v, isCompressed: %v, imageLength: %v", err.hasHole, err.isCompressed, err.length)
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
		return InconsistentBlockImageHoleStateError{imageHeader.HoleOffset, imageHeader.HoleLength,
			imageHeader.ImageLength, imageHeader.HasHole()}
	}
	return nil
}

func (imageHeader *XLogRecordBlockImageHeader) checkLengthConsistency() error {
	if (imageHeader.IsCompressed() && imageHeader.ImageLength == BlockSize) ||
		(!imageHeader.HasHole() && !imageHeader.IsCompressed() && imageHeader.ImageLength != BlockSize) {
		return InconsistentBlockImageLengthError{imageHeader.HasHole(), imageHeader.IsCompressed(), imageHeader.ImageLength}
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
