package walparser

import (
	"bytes"
	"github.com/wal-g/wal-g/testingutil"
	"testing"
)

func TestReadXLogPageHeader_Long(t *testing.T) {
	headerData := []byte{
		0x98, 0xd0, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00,
		0xcd, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa9, 0xc7, 0x7b, 0xc1, 0x0d, 0x5a, 0x38, 0x5b,
		0x00, 0x00, 0x00, 0x01,
	}
	reader := bytes.NewReader(headerData)
	header, err := readXLogPageHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, header.Magic, uint16(0xd098))
	testingutil.AssertEquals(t, header.Info, uint16(0x0007))
	testingutil.AssertEquals(t, header.TimeLineID, TimeLineID(0x00000001))
	testingutil.AssertEquals(t, header.PageAddress, XLogRecordPtr(0x000000002b000000))
	testingutil.AssertEquals(t, header.RemainingDataLen, uint32(0x00000acd))
	testingutil.AssertReaderIsEmpty(t, reader)
}

func TestReadXLogPageHeader_Short(t *testing.T) {
	headerData := []byte{
		0x98, 0xd0, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00,
		0xcd, 0x0a, 0x00, 0x00,
	}
	reader := bytes.NewReader(headerData)
	header, err := readXLogPageHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, header.Magic, uint16(0xd098))
	testingutil.AssertEquals(t, header.Info, uint16(0x0005))
	testingutil.AssertEquals(t, header.TimeLineID, TimeLineID(0x00000001))
	testingutil.AssertEquals(t, header.PageAddress, XLogRecordPtr(0x000000002b000000))
	testingutil.AssertEquals(t, header.RemainingDataLen, uint32(0x00000acd))
	testingutil.AssertReaderIsEmpty(t, reader)
}

func TestTryReadXLogRecordData_PartialHeader(t *testing.T) {
	data := []byte{
		0x01, 0x02, 0x03, 0x04,
	}
	alignedReader := NewAlignedReader(bytes.NewReader(data), 3)
	recordPart, whole, err := tryReadXLogRecordData(alignedReader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, whole, false)
	testingutil.AssertByteSlicesEqual(t, data, recordPart)
}

func TestTryReadXLogRecordData_PartialContent(t *testing.T) {
	data := []byte{
		0x05, 0x1d, 0x00, 0x00, 0x43, 0x02, 0x00, 0x00, 0xc8, 0xed, 0xff, 0x2a, 0x00, 0x00, 0x00, 0x00,
		0xb0, 0x00, 0x00, 0x00, 0x3c, 0x20, 0xf5, 0xec,
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	}
	alignedReader := NewAlignedReader(bytes.NewReader(data), 3)
	recordPart, whole, err := tryReadXLogRecordData(alignedReader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, whole, false)
	testingutil.AssertByteSlicesEqual(t, data, recordPart)
}

func TestTryReadXLogRecordData_FullRecord(t *testing.T) {
	data := []byte{
		0x1a, 0x00, 0x00, 0x00, 0x43, 0x02, 0x00, 0x00, 0xc8, 0xed, 0xff, 0x2a, 0x00, 0x00, 0x00, 0x00,
		0xb0, 0x00, 0x00, 0x00, 0x3c, 0x20, 0xf5, 0xec,
		0x00, 0x01,
	}
	alignedReader := NewAlignedReader(bytes.NewReader(data), 3)
	recordPart, whole, err := tryReadXLogRecordData(alignedReader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, whole, true)
	testingutil.AssertByteSlicesEqual(t, data, recordPart)
}
