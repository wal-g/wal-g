package walparser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"io"
	"io/ioutil"
)

const (
	WalPageSize         uint16 = 8192
	BlockSize           uint16 = 8192
	XLogRecordAlignment        = 8
)

type ZeroPageError struct {
	error
}

func NewZeroPageError() ZeroPageError {
	return ZeroPageError{errors.New("the whole page consists only of zero bytes")}
}

func (err ZeroPageError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type CantSavePartialParserError struct {
	error
}

func NewCantSavePartialParserError() CantSavePartialParserError {
	return CantSavePartialParserError{errors.New("wal parser doesn't contain beginning of saved record, so it's invalid")}
}

func (err CantSavePartialParserError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type PartialPageError struct {
	error
}

func NewPartialPageError() PartialPageError {
	return PartialPageError{errors.New("the page is partial, maybe it is the last non zero page of .partial file")}
}

func (err PartialPageError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalParser struct {
	currentRecordData         []byte
	hasCurrentRecordBeginning bool
}

func NewWalParser() *WalParser {
	return &WalParser{make([]byte, 0), false}
}

func (parser *WalParser) setCurrentRecordData(data []byte) {
	parser.currentRecordData = data
	parser.hasCurrentRecordBeginning = len(data) > 0
}

func (parser *WalParser) Invalidate() {
	parser.setCurrentRecordData(nil)
}

// For now we suppose that no wal record crosses whole wal page.
// If there is no currentRecordData (e. g. we look at the first record in the file), then we return
// prevRecordTail and discard it in parser.
func (parser *WalParser) ParseRecordsFromPage(reader io.Reader) (prevRecordTail []byte, pageRecords []XLogRecord, err error) {
	// returning pageParsingErr later is important because of PartialPageError possibility
	page, pageParsingErr := parser.parsePage(reader)
	if _, ok := pageParsingErr.(PartialPageError); !ok && pageParsingErr != nil {
		return nil, nil, pageParsingErr
	}
	if uint32(len(page.PrevRecordTrailingData)) < page.Header.RemainingDataLen {
		// ok, it's not all
		parser.currentRecordData = concatByteSlices(parser.currentRecordData, page.PrevRecordTrailingData)
		return nil, nil, pageParsingErr
	}
	currentRecordData := concatByteSlices(parser.currentRecordData, page.PrevRecordTrailingData)
	if !parser.hasCurrentRecordBeginning {
		parser.setCurrentRecordData(page.NextRecordHeadingData)
		return currentRecordData, page.Records, pageParsingErr
	}
	header, err := readXLogRecordHeader(bytes.NewReader(currentRecordData))
	if err != nil {
		return nil, nil, err
	}
	if header.TotalRecordLength != uint32(len(currentRecordData)) {
		return nil, nil, NewContinuationNotFoundError()
	}
	currentRecord, err := ParseXLogRecordFromBytes(currentRecordData)
	if err != nil {
		return nil, nil, err
	}
	records := make([]XLogRecord, len(page.Records)+1)
	records[0] = *currentRecord
	copy(records[1:], page.Records)
	parser.setCurrentRecordData(page.NextRecordHeadingData)
	return nil, records, pageParsingErr
}

func (parser *WalParser) parsePage(reader io.Reader) (*XLogPage, error) {
	alignedReader := NewAlignedReader(reader, XLogRecordAlignment)
	pageHeader, err := readXLogPageHeader(alignedReader)
	if err != nil {
		if _, ok := err.(ZeroPageHeaderError); ok {
			pageData, err1 := ioutil.ReadAll(alignedReader)
			if err1 != nil {
				return nil, errors.WithStack(err1)
			}
			if allZero(pageData) {
				return nil, NewZeroPageError()
			}
		}
		return nil, err
	}
	err = alignedReader.ReadToAlignment()
	if err != nil {
		return nil, err
	}
	remainingData := make([]byte, minUint32(pageHeader.RemainingDataLen, uint32(WalPageSize)))
	readCount, err := alignedReader.Read(remainingData)
	if err != nil && errors.Cause(err) != io.EOF {
		return nil, err
	}
	if uint32(readCount) != pageHeader.RemainingDataLen {
		return &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData[:readCount]}, nil
	}
	// if remainingData can be a part of WAL-switch record and we can check it
	if parser.hasCurrentRecordBeginning {
		record, err := ParseXLogRecordFromBytes(concatByteSlices(parser.currentRecordData, remainingData))
		if err != nil {
			return nil, err
		}
		if record.isWALSwitch() {
			return &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData}, nil
		}
	}
	pageRecords := make([]XLogRecord, 0)
	for {
		recordData, wholeRecord, err := tryReadXLogRecordData(alignedReader)
		if err != nil {
			return checkPartialPage(alignedReader, &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData, Records: pageRecords}, err)
		}
		if wholeRecord {
			// The header was previously validated being zero, so now it doesn't need to. However we do this for code robustness.
			record, err := ParseXLogRecordFromBytes(recordData)
			if err != nil {
				return checkPartialPage(alignedReader, &XLogPage{Header: *pageHeader, PrevRecordTrailingData: remainingData, Records: pageRecords}, err)
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

func checkPartialPage(pageReader io.Reader, page *XLogPage, recordReadingErr error) (*XLogPage, error) {
	if _, ok := recordReadingErr.(ZeroRecordHeaderError); ok {
		pageData, err1 := ioutil.ReadAll(pageReader)
		if err1 != nil {
			return nil, errors.WithStack(err1)
		}
		if allZero(pageData) {
			return page, NewPartialPageError()
		}
	}
	return nil, errors.WithStack(recordReadingErr)
}

func (parser *WalParser) Save(writer io.Writer) error {
	if len(parser.currentRecordData) > 0 && !parser.hasCurrentRecordBeginning {
		return NewCantSavePartialParserError()
	}
	currentRecordDataLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(currentRecordDataLen, uint32(len(parser.currentRecordData)))
	_, err := writer.Write(currentRecordDataLen)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = writer.Write(parser.currentRecordData)
	return errors.WithStack(err)
}

func (parser *WalParser) GetCurrentRecordData() []byte {
	return parser.currentRecordData
}

func LoadWalParser(reader io.Reader) (*WalParser, error) {
	var dataLen uint32
	err := parsingutil.NewFieldToParse(&dataLen, "record data prefix len").ParseFrom(reader)
	if err != nil {
		return nil, err
	}
	data := make([]byte, dataLen)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &WalParser{data, len(data) > 0}, nil
}

func LoadWalParserFromCurrentRecordHead(currentRecordHead []byte) *WalParser {
	return &WalParser{currentRecordHead, true}
}
