package walparser

import (
	"bytes"
	"io"
	"io/ioutil"
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
		parser.currentRecordData = page.NextRecordHeadingData
		return page.Records, nil
	} else {
		currentRecordData := concatByteSlices(parser.currentRecordData, page.PrevRecordTrailingData)
		header, err := readXLogRecordHeader(bytes.NewReader(currentRecordData))
		if err != nil {
			return nil, err
		}
		if header.TotalRecordLength == uint32(len(currentRecordData)) {
			currentRecord, err := parseXLogRecordFromBytes(currentRecordData)
			if err != nil {
				return nil, err
			}
			records := make([]XLogRecord, len(page.Records)+1)
			records[0] = *currentRecord
			copy(records[1:], page.Records)
			parser.currentRecordData = page.NextRecordHeadingData
			return records, nil
		}
		if len(page.Records) != 0 || len(page.NextRecordHeadingData) != 0 {
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
	remainingData := make([]byte, minUint32(pageHeader.RemainingDataLen, uint32(WalPageSize)))
	readCount, err := alignedReader.Read(remainingData)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if uint32(readCount) != pageHeader.RemainingDataLen {
		return &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData[:readCount]}, nil
	}
	// if remainingData can be a part of WAL-switch record and we can check it
	if len(parser.currentRecordData) > 0 {
		record, err := parseXLogRecordFromBytes(concatByteSlices(parser.currentRecordData, remainingData))
		if err != nil {
			return nil, err
		}
		if record.isWALSwitch() {
			return &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData}, nil
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
				return &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData, Records: pageRecords}, nil
			}
			continue
		}
		return &XLogPage{*pageHeader, remainingData, pageRecords, recordData}, nil
	}
}

func NewWalParser() *WalParser {
	return &WalParser{nil}
}

func (parser *WalParser) SaveParser(writer io.Writer) error {
	_, err := writer.Write(parser.currentRecordData)
	return err
}

func LoadParser(reader io.Reader) (*WalParser, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return &WalParser{data}, nil
}
