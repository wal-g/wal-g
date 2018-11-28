package walparser

const (
	XlrMaxBlockId       = 32
	XlrBlockIdDataShort = 255
	XlrBlockIdDataLong  = 254
	XlrBlockIdOrigin    = 253

	BkpBlockForkMask uint8 = 0x0F
	BkpBlockFlagMask uint8 = 0xF0
	BkpBlockHasImage uint8 = 0x10
	BkpBlockHasData  uint8 = 0x20
	BkpBlockWillInit uint8 = 0x40
	BkpBlockSameRel  uint8 = 0x80
)

type XLogRecordBlockHeader struct {
	BlockId       uint8
	ForkFlags     uint8
	DataLength    uint16
	ImageHeader   XLogRecordBlockImageHeader
	BlockLocation BlockLocation
}

func NewXLogRecordBlockHeader(blockId uint8) *XLogRecordBlockHeader {
	return &XLogRecordBlockHeader{BlockId: blockId}
}

func (blockHeader *XLogRecordBlockHeader) ForkNum() uint8 {
	return blockHeader.ForkFlags & BkpBlockForkMask
}

func (blockHeader *XLogRecordBlockHeader) HasImage() bool {
	return (blockHeader.ForkFlags & BkpBlockHasImage) != 0
}

func (blockHeader *XLogRecordBlockHeader) HasData() bool {
	return (blockHeader.ForkFlags & BkpBlockHasData) != 0
}

func (blockHeader *XLogRecordBlockHeader) WillInit() bool {
	return (blockHeader.ForkFlags & BkpBlockWillInit) != 0
}

func (blockHeader *XLogRecordBlockHeader) HasSameRel() bool {
	return (blockHeader.ForkFlags & BkpBlockSameRel) != 0
}

func (blockHeader *XLogRecordBlockHeader) checkDataStateConsistency() error {
	if (blockHeader.HasData() && blockHeader.DataLength == 0) ||
		(!blockHeader.HasData() && blockHeader.DataLength != 0) {
		return NewInconsistentBlockDataStateError(blockHeader.HasData(), blockHeader.DataLength)
	}
	return nil
}
