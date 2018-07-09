package wal_parser

type XLogRecord struct {
	header *XLogRecordHeader
	mainDataLen uint32
	origin      uint16
	blocks []XLogRecordBlock
	mainData []byte
}

func NewXLogRecord(header *XLogRecordHeader) XLogRecord {
	blocks := make([]XLogRecordBlock, 0)
	return XLogRecord{header: header, blocks: blocks}
}
