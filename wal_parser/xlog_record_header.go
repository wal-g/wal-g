package wal_parser

const (
	// info flags

	XlrInfoMask = 0x0F
	XlrRmgrInfoMask	= 0xF0

	XlrSpecialRelUpdate  = 0x01
	XlrCheckConsistency  = 0x02
	XLogRecordHeaderSize = 24
)


/* This struct corresponds to postgres struct XLogRecord.
 * For clarification you can find it in postgres:
 * src/include/access/xlogrecord.h
 */
type XLogRecordHeader struct {
	totalLength uint32
	xactID uint32
	prevRecordPtr uint64
	info uint8
	resourceManagerID uint8
	/* 2 bytes of padding here, initialize to zero */
	crc32Hash uint32
	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
}