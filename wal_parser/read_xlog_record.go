package wal_parser

import (
	"bytes"
	"io"
)

func readXLogRecordHeader(reader io.Reader) (*XLogRecordHeader, error) {
	xLogRecordHeader := XLogRecordHeader{}
	err := parseMultipleFieldsFromReader([]FieldToParse{
		{&xLogRecordHeader.totalRecordLength, "totalRecordLength"},
		{&xLogRecordHeader.xactID, "xactID"},
		{&xLogRecordHeader.prevRecordPtr, "prevRecordPtr"},
		{&xLogRecordHeader.info, "info"},
		{&xLogRecordHeader.resourceManagerID, "resourceManagerID"},
		PaddingByte,
		PaddingByte,
		{&xLogRecordHeader.crc32Hash, "crc32Hash"},
	}, reader)
	if err != nil {
		return nil, err
	}
	err = xLogRecordHeader.checkConsistency()
	if err != nil {
		return nil, err
	}
	return &xLogRecordHeader, nil
}

func readRelFileNode(reader io.Reader) (*RelFileNode, error) {
	relFileNode := RelFileNode{}
	err := parseMultipleFieldsFromReader([]FieldToParse{
		{&relFileNode.spcNode, "spcNode"},
		{&relFileNode.dbNode, "dbNode"},
		{&relFileNode.relNode, "relNode"},
	}, reader)
	if err != nil {
		return nil, err
	}
	return &relFileNode, nil
}

func parseXLogRecordFromBytes(data []byte) (*XLogRecord, error) {
	reader := bytes.NewReader(data)
	header, err := readXLogRecordHeader(reader)
	if err != nil {
		return nil, err
	}
	return readXLogRecordBody(header, reader)
}

func readXLogRecordBlockDataAndImages(record *XLogRecord, reader io.Reader) error {
	for i := range record.blocks {
		block := &record.blocks[i]
		if (*block).header.hasImage() {
			(*block).image = make([]byte, (*block).header.imageHeader.imageLength)
			_, err := io.ReadFull(reader, (*block).image)
			if err != nil {
				return err
			}
		}
		if (*block).header.hasData() {
			(*block).data = make([]byte, (*block).header.dataLength)
			_, err := io.ReadFull(reader, (*block).data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readXLogRecordBlockImageHeader(reader io.Reader) (*XLogRecordBlockImageHeader, error) {
	blockImageHeader := XLogRecordBlockImageHeader{}
	err := parseMultipleFieldsFromReader([]FieldToParse{
		{&blockImageHeader.imageLength, "imageLength"},
		{&blockImageHeader.holeOffset, "imageHoleOffset"},
		{&blockImageHeader.info, "imageInfo"},
	}, reader)
	if err != nil {
		return nil, err
	}
	if blockImageHeader.isCompressed() {
		if blockImageHeader.hasHole() {
			err = NewFieldToParse(&blockImageHeader.holeLength, "imageHoleLength").parseFrom(reader)
			if err != nil {
				return nil, err
			}
		}
	} else {
		blockImageHeader.holeLength = BlockSize - blockImageHeader.imageLength
	}
	err = blockImageHeader.checkConsistency()
	if err != nil {
		return nil, err
	}
	return &blockImageHeader, nil
}

func readBlockLocation(blockHasSameRel bool, lastRelFileNode *RelFileNode, reader io.Reader) (location *BlockLocation, err error) {
	var relFileNode *RelFileNode
	if blockHasSameRel {
		if lastRelFileNode == nil {
			return nil, NoPrevRelFileNodeError
		}
		relFileNode = lastRelFileNode
	} else {
		relFileNode, err = readRelFileNode(reader)
		if err != nil {
			return
		}
		lastRelFileNode = relFileNode
	}
	var blockNo uint32
	err = NewFieldToParse(&blockNo, "blockNo").parseFrom(reader)
	if err != nil {
		return
	}
	location = &BlockLocation{*relFileNode, blockNo}
	return
}

func readXLogRecordBlockHeader(lastRelFileNode *RelFileNode,
	blockId uint8, maxReadBlockId *int, reader *ShrinkableReader) (*XLogRecordBlockHeader, error) {
	if blockId > XlrMaxBlockId {
		return nil, InvalidRecordBlockIdError{blockId}
	}
	blockHeader := NewXLogRecordBlockHeader(blockId)
	if int(blockHeader.blockId) <= *maxReadBlockId {
		return nil, OutOfOrderBlockIdError{int(blockHeader.blockId), *maxReadBlockId}
	}
	*maxReadBlockId = int(blockHeader.blockId)

	err := parseMultipleFieldsFromReader([]FieldToParse{
		{&blockHeader.forkFlags, "forkFlags"},
		{&blockHeader.dataLength, "dataLength"},
	}, reader)
	if err != nil {
		return nil, err
	}
	err = blockHeader.checkDataStateConsistency()
	if err != nil {
		return nil, err
	}
	reader.Shrink(int(blockHeader.dataLength))

	if blockHeader.hasImage() {
		imageHeader, err := readXLogRecordBlockImageHeader(reader)
		blockHeader.imageHeader = *imageHeader
		if err != nil {
			return nil, err
		}
		err = reader.Shrink(int(blockHeader.imageHeader.imageLength))
		if err != nil {
			return nil, err
		}
	}

	blockLocation, err := readBlockLocation(blockHeader.hasSameRel(), lastRelFileNode, reader)
	if err != nil {
		return nil, err
	}
	blockHeader.blockLocation = *blockLocation
	return blockHeader, nil
}

func readXLogRecordBlockHeaderPart(record *XLogRecord, reader io.Reader) error {
	var lastRelFileNode *RelFileNode = nil
	var maxReadBlockId = -1
	headerReader := &ShrinkableReader{reader, int(record.header.totalRecordLength - XLogRecordHeaderSize)}
	for headerReader.dataRemained > 0 {
		var blockId uint8
		err := NewFieldToParse(&blockId, "blockId").parseFrom(headerReader)
		if err != nil {
			return err
		}
		switch blockId {
		case XlrBlockIdDataShort:
			var mainDataLen uint8
			err := NewFieldToParse(&mainDataLen, "mainDataLen8").parseFrom(headerReader)
			if err != nil {
				return err
			}
			record.mainDataLen = uint32(mainDataLen)
			headerReader.Shrink(int(mainDataLen))
		case XlrBlockIdDataLong:
			err := NewFieldToParse(&record.mainDataLen, "mainDataLen32").parseFrom(headerReader)
			if err != nil {
				return err
			}
			headerReader.Shrink(int(record.mainDataLen))
		case XlrBlockIdOrigin:
			err := NewFieldToParse(&record.origin, "origin").parseFrom(headerReader)
			if err != nil {
				return err
			}
		default:
			blockHeader, err := readXLogRecordBlockHeader(lastRelFileNode, blockId, &maxReadBlockId, headerReader)
			if err != nil {
				return err
			}
			record.blocks = append(record.blocks, XLogRecordBlock{header: *blockHeader})
		}
	}
	return nil
}

func readXLogRecordMainData(mainDataLen uint32, reader io.Reader) ([]byte, error) {
	mainData := make([]byte, mainDataLen)
	_, err := io.ReadFull(reader, mainData)
	return mainData, err
}

func readXLogRecordBody(header *XLogRecordHeader, reader io.Reader) (*XLogRecord, error) {
	record := NewXLogRecord(*header)
	readXLogRecordBlockHeaderPart(record, reader)

	err := readXLogRecordBlockDataAndImages(record, reader)
	if err != nil {
		return nil, err
	}

	record.mainData, err = readXLogRecordMainData(record.mainDataLen, reader)
	if err != nil {
		return nil, err
	}
	return record, nil
}
