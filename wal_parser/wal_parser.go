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
		if header.totalLength == uint32(len(parser.currentRecordData) + len(page.prevRecordTrailingData)) {
			currentRecord, err := parseXLogRecordFromBytes(concatBytes(parser.currentRecordData, page.prevRecordTrailingData))
			if err != nil {
				return nil, err
			}
			records := make([]XLogRecord, len(page.records) + 1)
			records[0] = *currentRecord
			copy(records[1:], page.records)
			return records, nil
		}
		if len(page.records) != 0 || len(page.nextRecordHeadingData) != 0 {
			return nil, ContinuationNotFoundError{}
		}
		parser.currentRecordData = concatBytes(parser.currentRecordData, page.prevRecordTrailingData)
		return make([]XLogRecord, 0), nil
	}
}

func tryReadXLogRecordData(alignedReader *AlignedReader) ([]byte, bool, error) {
	err := alignedReader.ReadToAlignment()
	if err != nil {
		if err == io.EOF {
			return nil, false, nil
		}
		return nil, false, err
	}
	headerData := make([]byte, XLogRecordHeaderSize)
	readCount, err := alignedReader.Read(headerData)
	if err != nil {
		if err == io.EOF {
			return headerData[:readCount], false, nil
		}
		return nil, false, err
	}
	recordHeader, err := readXLogRecordHeader(bytes.NewReader(headerData))
	if err != nil {
		return nil, false, err
	}
	recordContent := make([]byte, minUint32(recordHeader.totalLength - XLogRecordHeaderSize, uint32(WalPageSize)))
	readCount, err = alignedReader.Read(recordContent)
	if err != nil {
		if err == io.EOF {
			return concatBytes(headerData, recordContent[:readCount]), false, nil
		}
		return nil, false, err
	}
	return concatBytes(headerData, recordContent[:readCount]), true, nil
}

func parsePage(reader io.Reader) (*XLogPage, error) {
	alignedReader := NewAlignedReader(reader, XLogRecordAlignment)
	pageHeader, err := readXLogPageHeader(alignedReader)
	if err != nil {
		return nil, err
	}
	remainingData := make([]byte, minUint32(pageHeader.remainingDataLen, uint32(WalPageSize)))
	_, err = alignedReader.Read(remainingData)
	if err != nil {
		if err == io.EOF {
			return &XLogPage{header: *pageHeader, prevRecordTrailingData: remainingData}, nil
		}
		return nil, err
	}
	pageRecords := make([]XLogRecord, 0)
	for {
		recordData, success, err := tryReadXLogRecordData(alignedReader)
		if err != nil {
			return nil, err
		}
		if success {
			record, err := parseXLogRecordFromBytes(recordData)
			if err != nil {
				return nil, err
			}
			pageRecords = append(pageRecords, *record)
			continue
		}
		return &XLogPage{*pageHeader, remainingData, pageRecords, recordData}, nil
	}
}

func readXLogLongPageHeaderData(reader io.Reader) error {
	var systemID uint64
	var segmentSize uint32
	var xLogBlockSize uint32
	return parseMultipleFieldsFromReader([]FieldToParse {
		{&systemID, "systemID"},
		{&segmentSize, "segmentSize"},
		{xLogBlockSize, "xLogBlockSize"},
	}, reader)
}

// If header is long, then long header data is read from reader and thrown away
func readXLogPageHeader(reader io.Reader) (*XLogPageHeader, error) {
	pageHeader := XLogPageHeader{}
	err := parseMultipleFieldsFromReader([]FieldToParse {
		{&pageHeader.magic, "magic"},
		{&pageHeader.info, "info"},
		{&pageHeader.timeLineID, "timeLineID"},
		{&pageHeader.pageAddress, "pageAddress"},
		{&pageHeader.remainingDataLen, "remainingDataLen"},
	}, reader)
	if err != nil {
		return nil, err
	}

	// read long header data from reader
	if pageHeader.isLong() {
		err = readXLogLongPageHeaderData(reader)
		if err != nil {
			return nil, err
		}
	}

	return &pageHeader, nil
}
