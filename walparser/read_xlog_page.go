package walparser

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
)

func tryReadXLogRecordData(alignedReader *AlignedReader) (data []byte, whole bool, err error) {
	err = alignedReader.ReadToAlignment()
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
		return headerData[:readCount], false, nil // header don't fit into the page
	}
	recordHeader, err := readXLogRecordHeader(bytes.NewReader(headerData))
	if err != nil {
		return nil, false, err
	}
	recordContent := make([]byte, minUint32(recordHeader.TotalRecordLength-XLogRecordHeaderSize, uint32(WalPageSize)))
	readCount, err = alignedReader.Read(recordContent)
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	wholeRecord := uint32(readCount) == recordHeader.TotalRecordLength-XLogRecordHeaderSize
	return concatByteSlices(headerData, recordContent[:readCount]), wholeRecord, nil
}

func readXLogLongPageHeaderData(reader io.Reader) error {
	var systemID uint64
	var segmentSize uint32
	var xLogBlockSize uint32
	return parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		*parsingutil.NewFieldToParse(&systemID, "systemID"),
		*parsingutil.NewFieldToParse(&segmentSize, "segmentSize"),
		*parsingutil.NewFieldToParse(&xLogBlockSize, "xLogBlockSize"),
	}, reader)
}

// If header is long, then long header data is read from reader and thrown away
func readXLogPageHeader(reader io.Reader) (*XLogPageHeader, error) {
	pageHeader := XLogPageHeader{}
	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		*parsingutil.NewFieldToParse(&pageHeader.Magic, "magic"),
		*parsingutil.NewFieldToParse(&pageHeader.Info, "info"),
		*parsingutil.NewFieldToParse(&pageHeader.TimeLineID, "timeLineID"),
		*parsingutil.NewFieldToParse(&pageHeader.PageAddress, "pageAddress"),
		*parsingutil.NewFieldToParse(&pageHeader.RemainingDataLen, "remainingDataLen"),
	}, reader)
	if err != nil {
		return nil, err
	}

	// read long header data from reader
	if pageHeader.IsLong() {
		err = readXLogLongPageHeaderData(reader)
		if err != nil {
			return nil, err
		}
	}

	return &pageHeader, nil
}
