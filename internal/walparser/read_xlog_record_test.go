package walparser

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadXLogRecordHeader(t *testing.T) {
	headerData := []byte{
		0x05, 0x1d, 0x00, 0x00, 0x43, 0x02, 0x00, 0x00, 0xc8, 0xed, 0xff, 0x2a, 0x00, 0x00, 0x00, 0x00,
		0xb0, 0x00, 0x00, 0x00, 0x3c, 0x20, 0xf5, 0xec,
	}
	reader := bytes.NewReader(headerData)
	header, err := readXLogRecordHeader(reader)
	assert.NoError(t, err)
	assert.Equal(t, header.TotalRecordLength, uint32(0x00001d05))
	assert.Equal(t, header.XactID, uint32(0x00000243))
	assert.Equal(t, header.PrevRecordPtr, XLogRecordPtr(0x000000002affedc8))
	assert.Equal(t, header.Info, uint8(0xb0))
	assert.Equal(t, header.ResourceManagerID, uint8(0x00))
	assert.Equal(t, header.Crc32Hash, uint32(0xecf5203c))
	AssertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockHeader(t *testing.T) {
	maxReadBlockId := -1
	headerData := []byte{
		0x10, 0x00, 0x00, 0xd4, 0x1c, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40, 0x00,
		0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
	}
	reader := ShrinkableReader{bytes.NewReader(headerData), len(headerData) + 0x1cd4}
	header, lastRelFileNode, err := readXLogRecordBlockHeader(nil, 0, &maxReadBlockId, &reader)
	assert.NoError(t, err)
	assert.Equal(t, *lastRelFileNode, header.BlockLocation.RelationFileNode)
	assert.Equal(t, header.BlockId, uint8(0))
	assert.Equal(t, header.ForkFlags, uint8(0x10))
	assert.Equal(t, header.DataLength, uint16(0x0000))
	assert.Equal(t, header.ImageHeader.ImageLength, uint16(0x1cd4))
	assert.Equal(t, header.ImageHeader.HoleOffset, uint16(0x05d4))
	assert.Equal(t, header.ImageHeader.Info, uint8(0x05))
	assert.Equal(t, header.ImageHeader.HoleLength, BlockSize-header.ImageHeader.ImageLength)
	assert.Equal(t, header.BlockLocation.RelationFileNode.SpcNode, Oid(0x0000067f))
	assert.Equal(t, header.BlockLocation.RelationFileNode.DBNode, Oid(0x00004000))
	assert.Equal(t, header.BlockLocation.RelationFileNode.RelNode, Oid(0x00004015))
	assert.Equal(t, header.BlockLocation.BlockNo, uint32(0x000018e4))
	AssertReaderIsEmpty(t, &reader)
}

func TestReadBlockLocation_WithDifferentRel(t *testing.T) {
	data := []byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	reader := bytes.NewReader(data)
	location, err := readBlockLocation(false, new(RelFileNode), reader)
	assert.NoError(t, err)
	assert.Equal(t, location.RelationFileNode.SpcNode, Oid(0x67452301))
	assert.Equal(t, location.RelationFileNode.DBNode, Oid(0xefcdab89))
	assert.Equal(t, location.RelationFileNode.RelNode, Oid(0x78563412))
	assert.Equal(t, location.BlockNo, uint32(0xf0debc9a))
	AssertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockImageHeader_NotCompressed(t *testing.T) {
	data := []byte{
		0x42, 0x10, 0x30, 0x00, 0x05,
	}
	reader := bytes.NewReader(data)
	header, err := readXLogRecordBlockImageHeader(reader)
	assert.NoError(t, err)
	assert.Equal(t, header.ImageLength, uint16(0x1042))
	assert.Equal(t, header.HoleOffset, uint16(0x0030))
	assert.Equal(t, header.Info, uint8(0x05))
	assert.Equal(t, header.HoleLength, uint16(0x0fbe))
	AssertReaderIsEmpty(t, reader)
}

func TestReadXLogRecordBlockImageHeader_Compressed(t *testing.T) {
	data := []byte{
		0x42, 0x10, 0x30, 0x00, 0x07, 0x92, 0x00,
	}
	reader := bytes.NewReader(data)
	header, err := readXLogRecordBlockImageHeader(reader)
	assert.NoError(t, err)
	assert.Equal(t, header.ImageLength, uint16(0x1042))
	assert.Equal(t, header.HoleOffset, uint16(0x0030))
	assert.Equal(t, header.Info, uint8(0x07))
	assert.Equal(t, header.HoleLength, uint16(0x0092))
	AssertReaderIsEmpty(t, reader)
}

func testReadXLogRecordBlockHeaderPartLogic(t *testing.T, data []byte, blockDataLen uint32) *XLogRecord {
	reader := bytes.NewReader(data)
	record := NewXLogRecord(XLogRecordHeader{TotalRecordLength: XLogRecordHeaderSize + uint32(len(data)) + blockDataLen})
	err := readXLogRecordBlockHeaderPart(record, reader)
	assert.NoError(t, err)
	AssertReaderIsEmpty(t, reader)
	return record
}

func TestReadXLogRecordBlockHeaderPart_DataShort(t *testing.T) {
	data := []byte{
		0xff, 0x12,
	}
	expectedMainDataLen := uint32(0x12)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen)
	assert.Equal(t, record.MainDataLen, expectedMainDataLen)
}

func TestReadXLogRecordBlockHeaderPart_DataLong(t *testing.T) {
	data := []byte{
		0xfe, 0x98, 0x76, 0x54, 0x32,
	}
	expectedMainDataLen := uint32(0x32547698)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen)
	assert.Equal(t, record.MainDataLen, uint32(expectedMainDataLen))
}

func TestReadXLogRecordBlockHeaderPart_RecordOrigin(t *testing.T) {
	data := []byte{
		0xfd, 0x01, 0xfe,
	}
	expectedOrigin := uint16(0xfe01)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, 0)
	assert.Equal(t, record.Origin, expectedOrigin)
}

func TestReadXLogRecordBlockHeaderPart_MultipleBlocks(t *testing.T) {
	data := []byte{
		0xfd, 0x01, 0xfe,
		0x00, 0x10, 0x00, 0x00, 0xd4, 0x1c, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
		0xff, 0x12,
	}
	expectedOrigin := uint16(0xfe01)
	expectedMainDataLen := uint32(0x12)
	expectedImageLength := uint16(0x1cd4)
	record := testReadXLogRecordBlockHeaderPartLogic(t, data, expectedMainDataLen+uint32(expectedImageLength))
	assert.Equal(t, record.Origin, expectedOrigin)
	assert.Equal(t, record.MainDataLen, expectedMainDataLen)
	assert.Equal(t, len(record.Blocks), 1)
	header := record.Blocks[0].Header
	assert.Equal(t, header.BlockId, uint8(0))
	assert.Equal(t, header.ForkFlags, uint8(0x10))
	assert.Equal(t, header.DataLength, uint16(0x0000))
	assert.Equal(t, header.ImageHeader.ImageLength, expectedImageLength)
	assert.Equal(t, header.ImageHeader.HoleOffset, uint16(0x05d4))
	assert.Equal(t, header.ImageHeader.Info, uint8(0x05))
	assert.Equal(t, header.ImageHeader.HoleLength, BlockSize-header.ImageHeader.ImageLength)
	assert.Equal(t, header.BlockLocation.RelationFileNode.SpcNode, Oid(0x0000067f))
	assert.Equal(t, header.BlockLocation.RelationFileNode.DBNode, Oid(0x00004000))
	assert.Equal(t, header.BlockLocation.RelationFileNode.RelNode, Oid(0x00004015))
	assert.Equal(t, header.BlockLocation.BlockNo, uint32(0x000018e4))
}

func TestReadXLogRecordBlockDataAndImages_OnlyData(t *testing.T) {
	data := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
	}
	reader := bytes.NewReader(data)
	record := XLogRecord{
		Blocks: []XLogRecordBlock{{Header: XLogRecordBlockHeader{
			ForkFlags:  BkpBlockHasData,
			DataLength: uint16(len(data)),
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	assert.NoError(t, err)
	assert.Equal(t, record.Blocks[0].Data, data)
	assert.Equal(t, 0, len(record.Blocks[0].Image))
}

func TestReadXLogRecordBlockDataAndImages_OnlyImage(t *testing.T) {
	image := []byte{
		0x06, 0x07, 0x08, 0x09, 0x0a,
	}
	reader := bytes.NewReader(image)
	record := XLogRecord{
		Blocks: []XLogRecordBlock{{Header: XLogRecordBlockHeader{
			ForkFlags:   BkpBlockHasImage,
			ImageHeader: XLogRecordBlockImageHeader{ImageLength: uint16(len(image))},
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	assert.NoError(t, err)
	assert.Equal(t, record.Blocks[0].Image, image)
	assert.Equal(t, 0, len(record.Blocks[0].Data))
}

func TestReadXLogRecordBlockDataAndImages_DataAndImage(t *testing.T) {
	imageData := []byte{
		0x10, 0x11, 0x12, 0x13,
	}
	blockData := []byte{
		0x14, 0x15, 0x16, 0x17, 0x18,
	}
	dataAndImage := concatByteSlices(imageData, blockData)
	imageLen := 4
	reader := bytes.NewReader(dataAndImage)
	record := XLogRecord{
		Blocks: []XLogRecordBlock{{Header: XLogRecordBlockHeader{
			ForkFlags:   BkpBlockHasImage | BkpBlockHasData,
			DataLength:  5,
			ImageHeader: XLogRecordBlockImageHeader{ImageLength: uint16(imageLen)},
		}}},
	}
	err := readXLogRecordBlockDataAndImages(&record, reader)
	assert.NoError(t, err)
	assert.Equal(t, record.Blocks[0].Image, imageData)
	assert.Equal(t, record.Blocks[0].Data, blockData)
}

func TestReadXLogRecordBody(t *testing.T) {
	imageData := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
	}
	blockData := []byte{
		0x0a, 0x0b, 0x0c,
	}
	mainData := []byte{
		0x0d, 0x0e, 0x0f, 0x10,
	}
	data := []byte{ // block header data
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
	record, err := readXLogRecordBody(&XLogRecordHeader{TotalRecordLength: uint32(int(XLogRecordHeaderSize) + len(data))}, reader)
	assert.NoError(t, err)
	assert.Equal(t, record.Origin, expectedOrigin)
	assert.Equal(t, record.MainDataLen, expectedMainDataLen)
	assert.Equal(t, len(record.Blocks), 1)
	header := record.Blocks[0].Header
	assert.Equal(t, header.BlockId, uint8(0))
	assert.Equal(t, header.ForkFlags, uint8(0x30))
	assert.Equal(t, header.DataLength, uint16(0x0003))
	assert.Equal(t, header.ImageHeader.ImageLength, expectedImageLength)
	assert.Equal(t, header.ImageHeader.HoleOffset, uint16(0x05d4))
	assert.Equal(t, header.ImageHeader.Info, uint8(0x05))
	assert.Equal(t, header.ImageHeader.HoleLength, BlockSize-header.ImageHeader.ImageLength)
	assert.Equal(t, header.BlockLocation.RelationFileNode.SpcNode, Oid(0x0000067f))
	assert.Equal(t, header.BlockLocation.RelationFileNode.DBNode, Oid(0x00004000))
	assert.Equal(t, header.BlockLocation.RelationFileNode.RelNode, Oid(0x00004015))
	assert.Equal(t, header.BlockLocation.BlockNo, uint32(0x000018e4))
	assert.Equal(t, record.MainData, mainData)
	assert.Equal(t, record.Blocks[0].Image, imageData)
	assert.Equal(t, record.Blocks[0].Data, blockData)
	AssertReaderIsEmpty(t, reader)
}
