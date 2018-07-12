package wal_parser

import (
	"io"
	"bytes"
)

const (
	WalPageSize uint16 = 8192
	BlockSize uint16 = 8192
	XLogRecordAlignment = 8
)

type WalParser struct {
	currentRecordData []byte
}

func (parser *WalParser) ParseRecordsFromPage(reader io.Reader) ([]XLogRecord, error) {
	page, err := parsePage(reader)
	if err != nil {
		return nil, err
	}
	if len(parser.currentRecordData) == 0 {
		parser.currentRecordData = page.nextRecordHeadingData
		return page.records, nil
	} else {
		header, _ := readXLogRecordHeader(bytes.NewReader(parser.currentRecordData))
		if header.totalRecordLength == uint32(len(parser.currentRecordData) + len(page.prevRecordTrailingData)) {
			currentRecord, err := parseXLogRecordFromBytes(concatByteSlices(parser.currentRecordData, page.prevRecordTrailingData))
			if err != nil {
				return nil, err
			}
			records := make([]XLogRecord, len(page.records) + 1)
			records[0] = *currentRecord
			copy(records[1:], page.records)
			return records, nil
		}
		if len(page.records) != 0 || len(page.nextRecordHeadingData) != 0 {
			return nil, ContinuationNotFoundError
		}
		parser.currentRecordData = concatByteSlices(parser.currentRecordData, page.prevRecordTrailingData)
		return make([]XLogRecord, 0), nil
	}
}
