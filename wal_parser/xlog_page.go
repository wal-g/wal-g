package wal_parser

type XLogPage struct {
	header                 XLogPageHeader
	prevRecordTrailingData []byte
	records                []XLogRecord
	nextRecordHeadingData  []byte
}
