package wal_parser

import (
	"io"
	"bytes"
)

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
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	if readCount < XLogRecordHeaderSize {
		return headerData[:readCount], false, nil
	}
	recordHeader, err := readXLogRecordHeader(bytes.NewReader(headerData))
	if err != nil {
		return nil, false, err
	}
	recordContent := make([]byte, minUint32(recordHeader.totalRecordLength - XLogRecordHeaderSize, uint32(WalPageSize)))
	readCount, err = alignedReader.Read(recordContent)
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	wholeRecord := uint32(readCount) == recordHeader.totalRecordLength - XLogRecordHeaderSize
	return concatByteSlices(headerData, recordContent[:readCount]), wholeRecord, nil
}

func parsePage(reader io.Reader) (*XLogPage, error) {
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
	pageRecords := make([]XLogRecord, 0)
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
