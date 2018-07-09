package wal_parser

const (
	BkpImageHasHole uint8 = 0x01
	BkpImageIsCompressed uint8 = 0x02
	BkpImageApply uint8 = 0x04
)

type XLogRecordBlockImageHeader struct {
	length uint16
	holeOffset uint16
	holeLength uint16
	info uint8
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
