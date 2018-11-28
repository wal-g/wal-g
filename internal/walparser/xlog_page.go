package walparser

type XLogPage struct {
	Header                 XLogPageHeader
	PrevRecordTrailingData []byte
	Records                []XLogRecord
	NextRecordHeadingData  []byte
}
