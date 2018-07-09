package wal_parser

import (
	"io"
	"bytes"
)

func readXLogRecordHeader(reader io.Reader) (*XLogRecordHeader, error) {
	xLogRecordHeader := XLogRecordHeader{}
	err := parseMultipleFieldsFromReader([]FieldToParse {
		{&xLogRecordHeader.totalLength, "totalLength"},
		{&xLogRecordHeader.xactID, "xactID"},
		{&xLogRecordHeader.prevRecordPtr, "prevRecordPtr"},
		{&xLogRecordHeader.info, "info"},
		{&xLogRecordHeader.resourceManagerID, "resourceManagerID"},
		PaddingByte,
		PaddingByte,
		{&xLogRecordHeader.crc32Hash, "crc32Hash"},
	}, reader)
	return &xLogRecordHeader, err
}

func readRelFileNode(reader io.Reader) (*RelFileNode, error) {
	relFileNode := RelFileNode{}
	err := parseMultipleFieldsFromReader([]FieldToParse {
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
	record, err := readXLogRecord(header, reader)
	return record, err
}

func readXLogRecordBlocks(record *XLogRecord, reader io.Reader) error {
	for i := range record.blocks {
		block := &record.blocks[i]
		if (*block).header.hasImage() {
			(*block).image = make([]byte, (*block).header.imageHeader.length)
			_, err := reader.Read((*block).image)
			if err != nil {
				return err
			}
		}
		if (*block).header.hasData() {
			(*block).data = make([]byte, (*block).header.dataLength)
			_, err := reader.Read((*block).data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readXLogRecordBlockImageHeader(headersReader *ShrinkableReader) (*XLogRecordBlockImageHeader, error) {
	blockImageHeader := XLogRecordBlockImageHeader{}
	err := parseMultipleFieldsFromReader([]FieldToParse {
		{&blockImageHeader.length, "imageLength"},
		{&blockImageHeader.holeOffset, "imageHoleOffset"},
		{&blockImageHeader.info, "imageInfo"},
	}, headersReader)
	if err != nil {
		return nil, err
	}
	if blockImageHeader.isCompressed() {
		if blockImageHeader.hasHole() {
			err = FieldToParse{&blockImageHeader.holeLength, "imageHoleLength"}.parseFrom(headersReader)
			if err != nil {
				return nil, err
			}
		}
	} else {
		blockImageHeader.holeLength = BlockSize - blockImageHeader.length
	}
	headersReader.Shrink(int(blockImageHeader.length))
	if (blockImageHeader.hasHole() && (blockImageHeader.holeOffset == 0 || blockImageHeader.holeLength == 0 || blockImageHeader.length == BlockSize)) ||
		(!blockImageHeader.hasHole() && (blockImageHeader.holeOffset != 0 || blockImageHeader.holeLength != 0)) {
		return nil, InconsistentBlockImageHoleStateError{blockImageHeader.holeOffset, blockImageHeader.holeLength,
			blockImageHeader.length, blockImageHeader.hasHole()}
	}

	if (blockImageHeader.isCompressed() && blockImageHeader.length == BlockSize) ||
		(!blockImageHeader.hasHole() && !blockImageHeader.isCompressed() && blockImageHeader.length != BlockSize){
		return nil, InvalidBlockImageStateError{blockImageHeader.hasHole(), blockImageHeader.isCompressed(), blockImageHeader.length}
	}
	return &blockImageHeader, nil
}

func readBlockLocation(headersReader *ShrinkableReader, blockHeader *XLogRecordBlockHeader, lastRelFileNode *RelFileNode) (err error) {
	if blockHeader.hasSameRel() {
		if lastRelFileNode == nil {
			return NoPrevRelFileNodeError{}
		}
		blockHeader.relFileNode = lastRelFileNode
	} else {
		blockHeader.relFileNode, err = readRelFileNode(headersReader)
		if err != nil {
			return
		}
		lastRelFileNode = blockHeader.relFileNode
	}
	err = FieldToParse{&blockHeader.blockNo, "blockNo"}.parseFrom(headersReader)
	return
}

func readXLogRecordBlockHeader(headersReader *ShrinkableReader, lastRelFileNode *RelFileNode,
	blockId uint8, maxReadBlockId *uint8) (*XLogRecordBlockHeader, error) {
	blockHeader := NewXLogRecordBlockHeader(blockId)
	if blockHeader.blockId > XlrMaxBlockId {
		return nil, InvalidRecordBlockIdError{blockHeader.blockId}
	}
	if blockHeader.blockId <= *maxReadBlockId {
		return nil, OutOfOrderBlockIdError{blockHeader.blockId, *maxReadBlockId}
	}
	*maxReadBlockId = blockHeader.blockId
	err := parseMultipleFieldsFromReader([]FieldToParse {
		{&blockHeader.forkFlags, "forkFlags"},
		{&blockHeader.dataLength, "dataLength"},
	}, headersReader)
	if err != nil {
		return nil, err
	}
	if (blockHeader.hasData() && blockHeader.dataLength == 0) ||
		(!blockHeader.hasData() && blockHeader.dataLength != 0) {
		return nil, InconsistentBlockDataStateError{blockHeader.hasData(),  blockHeader.dataLength}
	}
	headersReader.Shrink(int(blockHeader.dataLength))
	if blockHeader.hasImage() {
		blockHeader.imageHeader, err = readXLogRecordBlockImageHeader(headersReader)
		if err != nil {
			return nil, err
		}
	}

	err = readBlockLocation(headersReader, &blockHeader, lastRelFileNode)
	if err != nil {
		return nil, err
	}
	return &blockHeader, nil
}

func readXLogRecord(header *XLogRecordHeader, reader io.Reader) (*XLogRecord, error) {
	var lastRelFileNode *RelFileNode = nil
	record := NewXLogRecord(header)
	var maxReadBlockId uint8 = 0
	headersReader := &ShrinkableReader{&reader, int(header.totalLength - XLogRecordHeaderSize)}
	for headersReader.dataRemained > 0 {
		var blockId uint8
		err := FieldToParse{&blockId, "blockId"}.parseFrom(headersReader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		switch blockId {
		case XlrBlockIdDataShort:
			var mainDataLen uint8
			err := FieldToParse{&mainDataLen, "mainDataLen8"}.parseFrom(headersReader)
			if err != nil {
				return nil, err
			}
			record.mainDataLen = uint32(mainDataLen)
		case XlrBlockIdDataLong:
			err := FieldToParse{&record.mainDataLen,  "mainDataLen32"}.parseFrom(headersReader)
			if err != nil {
				return nil, err
			}
		case XlrBlockIdOrigin:
			err := FieldToParse{&record.origin, "origin"}.parseFrom(headersReader)
			if err != nil {
				return nil, err
			}
		default:
			blockHeader, err := readXLogRecordBlockHeader(headersReader, lastRelFileNode, blockId, &maxReadBlockId)
			if err != nil {
				return nil, err
			}
			record.blocks = append(record.blocks, XLogRecordBlock{header: *blockHeader})
		}
	}

	err := readXLogRecordBlocks(&record, &reader)
	if err != nil {
		return nil, err
	}

	// the main data
	if record.mainDataLen == 0 {
		return &record, nil
	}
	record.mainData = make([]byte, record.mainDataLen)
	_, err = reader.Read(record.mainData)
	if err != nil {
		return nil, err
	}
	return &record, nil
}
