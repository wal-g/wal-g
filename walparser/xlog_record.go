package walparser

const (
	XLogSwitch          = 0x40
	WalSwitchRecordSize = XLogRecordHeaderSize
)

type XLogRecord struct {
	Header      XLogRecordHeader
	MainDataLen uint32
	Origin      uint16
	Blocks      []XLogRecordBlock
	MainData    []byte
}

func (record *XLogRecord) isWALSwitch() bool {
	return record.Header.ResourceManagerID == RmXlogID &&
		(record.Header.Info&^XlrInfoMask) == XLogSwitch
}

func NewXLogRecord(header XLogRecordHeader) *XLogRecord {
	blocks := make([]XLogRecordBlock, 0)
	return &XLogRecord{Header: header, Blocks: blocks}
}
