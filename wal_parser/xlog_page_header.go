package wal_parser

const (
	// info flags

	/* When record crosses page boundary, set this flag in new page's header */
	XlpFirstIsContRecord = 0x0001
	/* This flag indicates a "long" page header */
	XlpLongHeader = 0x0002
	/* This flag indicates backup blocks starting in this page are optional */
	XlpBkpRemovable = 0x0004
	/* All defined flag bits in xlp_info (used for validity checking of header) */
	XlpAllFlags = 0x0007
)

/* This struct corresponds to postgres struct XLogPageHeaderData.
 * For clarification you can find it in postgres:
 * src/include/access/xlog_internal.h
 */
type XLogPageHeader struct {
	magic uint16
	info uint16
	timeLineID uint32
	pageAddress uint64
	remainingDataLen uint32
}

func (pageHeader *XLogPageHeader) isLong() bool {
	return (pageHeader.info & XlpLongHeader) != 0
}
