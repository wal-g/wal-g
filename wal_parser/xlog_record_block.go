package wal_parser

type XLogRecordBlock struct {
	header XLogRecordBlockHeader
	image []byte
	data []byte
}
