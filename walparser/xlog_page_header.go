package walparser

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
	Magic            uint16
	Info             uint16
	TimeLineID       TimeLineID
	PageAddress      XLogRecordPtr
	RemainingDataLen uint32
}

func (pageHeader *XLogPageHeader) IsLong() bool {
	return (pageHeader.Info & XlpLongHeader) != 0
}

func (pageHeader *XLogPageHeader) isZero() bool {
	return pageHeader.Magic == 0 &&
		pageHeader.Info == 0 &&
		pageHeader.TimeLineID == 0 &&
		pageHeader.PageAddress == 0 &&
		pageHeader.RemainingDataLen == 0
}
