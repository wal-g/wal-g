package wal_parser

import (
	"testing"
	"bytes"
)

func TestReadXLogRecordHeader(t *testing.T) {
	headerData := []byte {
		0x05, 0x1d, 0x00, 0x00, 0x43, 0x02, 0x00, 0x00, 0xc8, 0xed, 0xff, 0x2a, 0x00, 0x00, 0x00, 0x00,
		0xb0, 0x00, 0x00, 0x00, 0x3c, 0x20, 0xf5, 0xec,
	}
	reader := bytes.NewReader(headerData)
	header, err := readXLogRecordHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, header.totalRecordLength, uint32(0x00001d05))
	assertEquals(t, header.xactID, uint32(0x00000243))
	assertEquals(t, header.prevRecordPtr, XLogRecordPtr(0x000000002affedc8))
	assertEquals(t, header.info, uint8(0xb0))
	assertEquals(t, header.resourceManagerID, uint8(0x00))
	assertEquals(t, header.crc32Hash, uint32(0xecf5203c))
	assertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockHeader(t *testing.T) {
	var lastRelFileNode *RelFileNode = nil
	maxReadBlockId := -1
	headerData := []byte {
		0x10, 0x00, 0x00, 0xd4, 0x1c, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40, 0x00,
		0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
	}
	reader := ShrinkableReader{bytes.NewReader(headerData), len(headerData) + 0x1cd4}
	header, err := readXLogRecordBlockHeader(lastRelFileNode, 0, &maxReadBlockId, &reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, header.blockId, uint8(0))
	assertEquals(t, header.forkFlags, uint8(0x10))
	assertEquals(t, header.dataLength, uint16(0x0000))
	assertEquals(t, header.imageHeader.imageLength, uint16(0x1cd4))
	assertEquals(t, header.imageHeader.holeOffset, uint16(0x05d4))
	assertEquals(t, header.imageHeader.info, uint8(0x05))
	assertEquals(t, header.imageHeader.holeLength, BlockSize - header.imageHeader.imageLength)
	assertEquals(t, header.blockLocation.relFileNode.spcNode, Oid(0x0000067f))
	assertEquals(t, header.blockLocation.relFileNode.dbNode, Oid(0x00004000))
	assertEquals(t, header.blockLocation.relFileNode.relNode, Oid(0x00004015))
	assertEquals(t, header.blockLocation.blockNo, uint32(0x000018e4))
	assertReaderIsEmpty(t, &reader)
}

func TestReadBlockLocation_WithDifferentRel(t *testing.T) {
	data := []byte {
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	reader := bytes.NewReader(data)
	location, err := readBlockLocation(false, nil, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, location.relFileNode.spcNode, Oid(0x67452301))
	assertEquals(t, location.relFileNode.dbNode, Oid(0xefcdab89))
	assertEquals(t, location.relFileNode.relNode, Oid(0x78563412))
	assertEquals(t, location.blockNo, uint32(0xf0debc9a))
	assertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockImageHeader_NotCompressed(t *testing.T) {
	data := []byte {
		0x42, 0x10, 0x30, 0x00, 0x05,
	}
	reader := bytes.NewReader(data)
	header, err := readXLogRecordBlockImageHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, header.imageLength, uint16(0x1042))
	assertEquals(t, header.holeOffset, uint16(0x0030))
	assertEquals(t, header.info, uint8(0x05))
	assertEquals(t, header.holeLength, uint16(0x0fbe))
	assertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockImageHeader_Compressed(t *testing.T) {
	data := []byte {
		0x42, 0x10, 0x30, 0x00, 0x07, 0x92, 0x00,
	}
	reader := bytes.NewReader(data)
	header, err := readXLogRecordBlockImageHeader(reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, header.imageLength, uint16(0x1042))
	assertEquals(t, header.holeOffset, uint16(0x0030))
	assertEquals(t, header.info, uint8(0x07))
	assertEquals(t, header.holeLength, uint16(0x0092))
	assertReaderIsEmpty(t, reader)
}

func testReadXLogRecordBlockHeaderPartLogic(t *testing.T, data []byte, blockDataLen uint32) *XLogRecord {
	reader := bytes.NewReader(data)
	record := NewXLogRecord(&XLogRecordHeader{totalRecordLength: XLogRecordHeaderSize + uint32(len(data)) + blockDataLen})
	err := readXLogRecordBlockHeaderPart(&record, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertReaderIsEmpty(t, reader)
	return &record
}

func TestReadXLogRecordBlockHeaderPart_DataShort(t *testing.T) {
	data := []byte {
		0xff, 0x12,
	}
	expectedMainDataLen := uint32(0x12)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen)
	assertEquals(t, record.mainDataLen, expectedMainDataLen)
}

func TestReadXLogRecordBlockHeaderPart_DataLong(t *testing.T) {
	data := []byte {
		0xfe, 0x98, 0x76, 0x54, 0x32,
	}
	expectedMainDataLen := uint32(0x32547698)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen)
	assertEquals(t, record.mainDataLen, uint32(expectedMainDataLen))
}

func TestReadXLogRecordBlockHeaderPart_RecordOrigin(t *testing.T) {
	data := []byte {
		0xfd, 0x01, 0xfe,
	}
	expectedOrigin := uint16(0xfe01)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, 0)
	assertEquals(t, record.origin, expectedOrigin)
}

func TestReadXLogRecordBlockHeaderPart_MultipleBlocks(t *testing.T) {
	data := []byte {
		0xfd, 0x01, 0xfe,
		0x00, 0x10, 0x00, 0x00, 0xd4, 0x1c, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
		0xff, 0x12,
	}
	expectedOrigin := uint16(0xfe01)
	expectedMainDataLen := uint32(0x12)
	expectedImageLength := uint16(0x1cd4)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen + uint32(expectedImageLength))
	assertEquals(t, record.origin, expectedOrigin)
	assertEquals(t, record.mainDataLen, expectedMainDataLen)
	assertEquals(t, len(record.blocks), 1)
	header := record.blocks[0].header
	assertEquals(t, header.blockId, uint8(0))
	assertEquals(t, header.forkFlags, uint8(0x10))
	assertEquals(t, header.dataLength, uint16(0x0000))
	assertEquals(t, header.imageHeader.imageLength, expectedImageLength)
	assertEquals(t, header.imageHeader.holeOffset, uint16(0x05d4))
	assertEquals(t, header.imageHeader.info, uint8(0x05))
	assertEquals(t, header.imageHeader.holeLength, BlockSize - header.imageHeader.imageLength)
	assertEquals(t, header.blockLocation.relFileNode.spcNode, Oid(0x0000067f))
	assertEquals(t, header.blockLocation.relFileNode.dbNode, Oid(0x00004000))
	assertEquals(t, header.blockLocation.relFileNode.relNode, Oid(0x00004015))
	assertEquals(t, header.blockLocation.blockNo, uint32(0x000018e4))
}

func TestReadXLogRecordBlockDataAndImages_OnlyData(t *testing.T) {
	data := []byte {
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
	}
	reader := bytes.NewReader(data)
	record := XLogRecord{
		blocks: []XLogRecordBlock {{header: XLogRecordBlockHeader{
			forkFlags: BkpBlockHasData,
			dataLength: uint16(len(data)),
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertByteSlicesEqual(t, record.blocks[0].data, data)
	assertEquals(t, 0, len(record.blocks[0].image))
}

func TestReadXLogRecordBlockDataAndImages_OnlyImage(t *testing.T) {
	image := []byte {
		0x06, 0x07, 0x08, 0x09, 0x0a,
	}
	reader := bytes.NewReader(image)
	record := XLogRecord{
		blocks: []XLogRecordBlock {{header: XLogRecordBlockHeader{
			forkFlags: BkpBlockHasImage,
			imageHeader: &XLogRecordBlockImageHeader{imageLength:uint16(len(image))},
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertByteSlicesEqual(t, record.blocks[0].image, image)
	assertEquals(t, 0, len(record.blocks[0].data))
}

func TestReadXLogRecordBlockDataAndImages_DataAndImage(t *testing.T) {
	imageData := []byte {
		0x10, 0x11, 0x12, 0x13,
	}
	blockData := []byte {
		0x14, 0x15, 0x16, 0x17, 0x18,
	}
	dataAndImage := concatByteSlices(imageData, blockData)
	imageLen := 4
	reader := bytes.NewReader(dataAndImage)
	record := XLogRecord{
		blocks: []XLogRecordBlock {{header: XLogRecordBlockHeader{
			forkFlags: BkpBlockHasImage | BkpBlockHasData,
			dataLength: 5,
			imageHeader: &XLogRecordBlockImageHeader{imageLength: uint16(imageLen)},
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertByteSlicesEqual(t, record.blocks[0].image, imageData)
	assertByteSlicesEqual(t, record.blocks[0].data, blockData)
}

func TestReadXLogRecordBody(t *testing.T) {
	imageData := []byte {
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
	}
	blockData := []byte {
		0x0a, 0x0b, 0x0c,
	}
	mainData := []byte {
		0x0d, 0x0e, 0x0f, 0x10,
	}
	data := []byte { // block header data
		0xfd, 0x01, 0xfe,
		0x00, 0x30, 0x03, 0x00, 0x0a, 0x00, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
		0xff, 0x04,
	}
	data = concatByteSlices(concatByteSlices(concatByteSlices(data, imageData), blockData), mainData)
	expectedOrigin := uint16(0xfe01)
	expectedMainDataLen := uint32(0x04)
	expectedImageLength := uint16(0x000a)
	reader := bytes.NewReader(data)
	record, err := readXLogRecordBody(&XLogRecordHeader{totalRecordLength: uint32(int(XLogRecordHeaderSize) + len(data))}, reader)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, record.origin, expectedOrigin)
	assertEquals(t, record.mainDataLen, expectedMainDataLen)
	assertEquals(t, len(record.blocks), 1)
	header := record.blocks[0].header
	assertEquals(t, header.blockId, uint8(0))
	assertEquals(t, header.forkFlags, uint8(0x30))
	assertEquals(t, header.dataLength, uint16(0x0003))
	assertEquals(t, header.imageHeader.imageLength, expectedImageLength)
	assertEquals(t, header.imageHeader.holeOffset, uint16(0x05d4))
	assertEquals(t, header.imageHeader.info, uint8(0x05))
	assertEquals(t, header.imageHeader.holeLength, BlockSize - header.imageHeader.imageLength)
	assertEquals(t, header.blockLocation.relFileNode.spcNode, Oid(0x0000067f))
	assertEquals(t, header.blockLocation.relFileNode.dbNode, Oid(0x00004000))
	assertEquals(t, header.blockLocation.relFileNode.relNode, Oid(0x00004015))
	assertEquals(t, header.blockLocation.blockNo, uint32(0x000018e4))
	assertByteSlicesEqual(t, record.mainData, mainData)
	assertByteSlicesEqual(t, record.blocks[0].image, imageData)
	assertByteSlicesEqual(t, record.blocks[0].data, blockData)
	assertReaderIsEmpty(t, reader)
}
