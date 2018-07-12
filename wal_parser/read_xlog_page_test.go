package wal_parser

import (
	"testing"
	"bytes"
)

// TODO : unit test short page header reading
func TestReadXLogLongPageHeader(t *testing.T) {
	headerData := []byte {
		0x98, 0xd0, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00,
		0xcd, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa9, 0xc7, 0x7b, 0xc1, 0x0d, 0x5a, 0x38, 0x5b,
		0x00, 0x00, 0x00, 0x01,
	}
	reader := bytes.NewReader(headerData)
	header, err := readXLogPageHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, header.magic, uint16(0xd098))
	assertEquals(t, header.info, uint16(0x0007))
	assertEquals(t, header.timeLineID, TimeLineID(0x00000001))
	assertEquals(t, header.pageAddress, XLogRecordPtr(0x000000002b000000))
	assertEquals(t, header.remainingDataLen, uint32(0x00000acd))
	assertReaderIsEmpty(t, reader)
}
