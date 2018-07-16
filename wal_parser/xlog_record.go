package wal_parser

const (
	XLogSwitch          = 0x40
	WalSwitchRecordSize = XLogRecordHeaderSize
)

type XLogRecord struct {
	header      XLogRecordHeader
	mainDataLen uint32
	origin      uint16
	blocks      []XLogRecordBlock
	mainData    []byte
}

func (record *XLogRecord) isWALSwitch() bool {
	return record.header.resourceManagerID == RmXlogID &&
		(record.header.info&^XlrInfoMask) == XLogSwitch
}

func NewXLogRecord(header XLogRecordHeader) *XLogRecord {
	blocks := make([]XLogRecordBlock, 0)
	return &XLogRecord{header: header, blocks: blocks}
}
