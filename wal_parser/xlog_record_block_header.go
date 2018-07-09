package wal_parser

const (
	XlrMaxBlockId = 32
	XlrBlockIdDataShort = 255
	XlrBlockIdDataLong = 254
	XlrBlockIdOrigin = 253

	BkpBlockForkMask uint8 = 0x0F
	BkpBlockFlagMask uint8 = 0xF0
	BkpBlockHasImage uint8 = 0x10
	BkpBlockHasData uint8 = 0x20
	BkpBlockWillInit uint8 = 0x40
	BkpBlockSameRel uint8 = 0x80
)

type XLogRecordBlockHeader struct {
	blockId uint8
	forkFlags uint8
	dataLength uint16
	imageHeader *XLogRecordBlockImageHeader
	relFileNode *RelFileNode
	blockNo uint32
}

func NewXLogRecordBlockHeader(blockId uint8) XLogRecordBlockHeader {
	return XLogRecordBlockHeader{blockId: blockId}
}

func (blockHeader *XLogRecordBlockHeader) forkNum() uint8 {
	return blockHeader.forkFlags & BkpBlockForkMask
}

func (blockHeader *XLogRecordBlockHeader) hasImage() bool {
	return (blockHeader.forkFlags & BkpBlockHasImage) != 0
}

func (blockHeader *XLogRecordBlockHeader) hasData() bool {
	return (blockHeader.forkFlags & BkpBlockHasData) != 0
}

func (blockHeader *XLogRecordBlockHeader) willInit() bool {
	return (blockHeader.forkFlags & BkpBlockWillInit) != 0
}

func (blockHeader *XLogRecordBlockHeader) hasSameRel() bool {
	return (blockHeader.forkFlags & BkpBlockSameRel) != 0
}
