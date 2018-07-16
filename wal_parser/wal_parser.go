package wal_parser

import (
	"bytes"
	"io"
)

const (
	WalPageSize         uint16 = 8192
	BlockSize           uint16 = 8192
	XLogRecordAlignment        = 8
)

type WalParser struct {
	currentRecordData []byte
}

func (parser *WalParser) ParseRecordsFromPage(reader io.Reader) ([]XLogRecord, error) {
	page, err := parser.parsePage(reader)
	if err != nil {
		return nil, err
	}
	if len(parser.currentRecordData) == 0 {
		parser.currentRecordData = page.nextRecordHeadingData
		return page.records, nil
	} else {
		currentRecordData := concatByteSlices(parser.currentRecordData, page.prevRecordTrailingData)
		header, err := readXLogRecordHeader(bytes.NewReader(currentRecordData))
		if err != nil {
			return nil, err
		}
		if header.totalRecordLength == uint32(len(currentRecordData)) {
			currentRecord, err := parseXLogRecordFromBytes(currentRecordData)
			if err != nil {
				return nil, err
			}
			records := make([]XLogRecord, len(page.records)+1)
			records[0] = *currentRecord
			copy(records[1:], page.records)
			parser.currentRecordData = page.nextRecordHeadingData
			return records, nil
		}
		if len(page.records) != 0 || len(page.nextRecordHeadingData) != 0 {
			return nil, ContinuationNotFoundError
		}
		parser.currentRecordData = currentRecordData
		return make([]XLogRecord, 0), nil
	}
}

func (parser *WalParser) parsePage(reader io.Reader) (*XLogPage, error) {
	alignedReader := NewAlignedReader(reader, XLogRecordAlignment)
	pageHeader, err := readXLogPageHeader(alignedReader)
	if err != nil {
		return nil, err
	}
	err = alignedReader.ReadToAlignment()
	if err != nil {
		return nil, err
	}
	remainingData := make([]byte, minUint32(pageHeader.remainingDataLen, uint32(WalPageSize)))
	readCount, err := alignedReader.Read(remainingData)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if uint32(readCount) != pageHeader.remainingDataLen {
		return &XLogPage{header: *pageHeader, prevRecordTrailingData: remainingData[:readCount]}, nil
	}
	// if remainingData can be a part of WAL-switch record and we can check it
	if len(parser.currentRecordData) > 0 {
		record, err := parseXLogRecordFromBytes(concatByteSlices(parser.currentRecordData, remainingData))
		if err != nil {
			return nil, err
		}
		if record.isWALSwitch() {
			return &XLogPage{header: *pageHeader, prevRecordTrailingData: remainingData}, nil
		}
	}
	pageRecords := make([]XLogRecord, 0) // TODO : also can be a tail of WAL switch
	for {
		recordData, wholeRecord, err := tryReadXLogRecordData(alignedReader)
		if err != nil {
			return nil, err
		}
		if wholeRecord {
			record, err := parseXLogRecordFromBytes(recordData)
			if err != nil {
				return nil, err
			}
			pageRecords = append(pageRecords, *record)
			if record.isWALSwitch() {
				return &XLogPage{header: *pageHeader, prevRecordTrailingData: remainingData, records: pageRecords}, nil
			}
			continue
		}
		return &XLogPage{*pageHeader, remainingData, pageRecords, recordData}, nil
	}
}
