package walparser

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
)

func readXLogRecordHeader(reader io.Reader) (*XLogRecordHeader, error) {
	xLogRecordHeader := XLogRecordHeader{}
	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &xLogRecordHeader.TotalRecordLength, Name: "totalRecordLength"},
		{Field: &xLogRecordHeader.XactID, Name: "xactID"},
		{Field: &xLogRecordHeader.PrevRecordPtr, Name: "prevRecordPtr"},
		{Field: &xLogRecordHeader.Info, Name: "info"},
		{Field: &xLogRecordHeader.ResourceManagerID, Name: "resourceManagerID"},
		parsingutil.PaddingByte,
		parsingutil.PaddingByte,
		{Field: &xLogRecordHeader.Crc32Hash, Name: "crc32Hash"},
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
	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &relFileNode.SpcNode, Name: "spcNode"},
		{Field: &relFileNode.DBNode, Name: "dbNode"},
		{Field: &relFileNode.RelNode, Name: "relNode"},
	}, reader)
	if err != nil {
		return nil, err
	}
	return &relFileNode, nil
}

func ParseXLogRecordFromBytes(data []byte) (*XLogRecord, error) {
	reader := bytes.NewReader(data)
	header, err := readXLogRecordHeader(reader)
	if err != nil {
		return nil, err
	}
	return readXLogRecordBody(header, reader)
}

func readXLogRecordBlockDataAndImages(record *XLogRecord, reader io.Reader) error {
	for i := range record.Blocks {
		block := &record.Blocks[i]
		if (*block).Header.HasImage() {
			(*block).Image = make([]byte, (*block).Header.ImageHeader.ImageLength)
			_, err := io.ReadFull(reader, (*block).Image)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		if (*block).Header.HasData() {
			(*block).Data = make([]byte, (*block).Header.DataLength)
			_, err := io.ReadFull(reader, (*block).Data)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func readXLogRecordBlockImageHeader(reader io.Reader) (*XLogRecordBlockImageHeader, error) {
	blockImageHeader := XLogRecordBlockImageHeader{}
	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &blockImageHeader.ImageLength, Name: "imageLength"},
		{Field: &blockImageHeader.HoleOffset, Name: "imageHoleOffset"},
		{Field: &blockImageHeader.Info, Name: "imageInfo"},
	}, reader)
	if err != nil {
		return nil, err
	}
	if blockImageHeader.IsCompressed() {
		if blockImageHeader.HasHole() {
			err = parsingutil.NewFieldToParse(&blockImageHeader.HoleLength, "imageHoleLength").ParseFrom(reader)
			if err != nil {
				return nil, err
			}
		}
	} else {
		blockImageHeader.HoleLength = BlockSize - blockImageHeader.ImageLength
	}
	err = blockImageHeader.checkConsistency()
	if err != nil {
		return nil, err
	}
	return &blockImageHeader, nil
}

func readBlockLocation(blockHasSameRel bool, lastRelFileNode **RelFileNode, reader io.Reader) (location *BlockLocation, err error) {
	var relFileNode *RelFileNode
	if blockHasSameRel {
		if *lastRelFileNode == nil {
			return nil, NewNoPrevRelFileNodeError()
		}
		relFileNode = *lastRelFileNode
	} else {
		relFileNode, err = readRelFileNode(reader)
		if err != nil {
			return
		}
		*lastRelFileNode = relFileNode
	}
	var blockNo uint32
	err = parsingutil.NewFieldToParse(&blockNo, "blockNo").ParseFrom(reader)
	if err != nil {
		return
	}
	location = &BlockLocation{*relFileNode, blockNo}
	return
}

func readXLogRecordBlockHeader(lastRelFileNode *RelFileNode,
	blockId uint8, maxReadBlockId *int, reader *ShrinkableReader) (*XLogRecordBlockHeader, error) {
	if blockId > XlrMaxBlockId {
		return nil, NewInvalidRecordBlockIdError(blockId)
	}
	blockHeader := NewXLogRecordBlockHeader(blockId)
	if int(blockHeader.BlockId) <= *maxReadBlockId {
		return nil, NewOutOfOrderBlockIdError(int(blockHeader.BlockId), *maxReadBlockId)
	}
	*maxReadBlockId = int(blockHeader.BlockId)

	err := parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &blockHeader.ForkFlags, Name: "forkFlags"},
		{Field: &blockHeader.DataLength, Name: "dataLength"},
	}, reader)
	if err != nil {
		return nil, err
	}
	err = blockHeader.checkDataStateConsistency()
	if err != nil {
		return nil, err
	}
	reader.Shrink(int(blockHeader.DataLength))

	if blockHeader.HasImage() {
		imageHeader, err := readXLogRecordBlockImageHeader(reader)
		if err != nil {
			return nil, err
		}
		blockHeader.ImageHeader = *imageHeader
		err = reader.Shrink(int(blockHeader.ImageHeader.ImageLength))
		if err != nil {
			return nil, err
		}
	}

	blockLocation, err := readBlockLocation(blockHeader.HasSameRel(), &lastRelFileNode, reader)
	if err != nil {
		return nil, err
	}
	blockHeader.BlockLocation = *blockLocation
	return blockHeader, nil
}

func readXLogRecordBlockHeaderPart(record *XLogRecord, reader io.Reader) error {
	var lastRelFileNode *RelFileNode = nil
	maxReadBlockId := -1
	headerReader := &ShrinkableReader{reader, int(record.Header.TotalRecordLength - XLogRecordHeaderSize)}
	for headerReader.dataRemained > 0 {
		var blockId uint8
		err := parsingutil.NewFieldToParse(&blockId, "blockId").ParseFrom(headerReader)
		if err != nil {
			return err
		}
		switch blockId {
		case XlrBlockIdDataShort:
			var mainDataLen uint8
			err := parsingutil.NewFieldToParse(&mainDataLen, "mainDataLen8").ParseFrom(headerReader)
			if err != nil {
				return err
			}
			record.MainDataLen = uint32(mainDataLen)
			headerReader.Shrink(int(mainDataLen))
		case XlrBlockIdDataLong:
			err := parsingutil.NewFieldToParse(&record.MainDataLen, "mainDataLen32").ParseFrom(headerReader)
			if err != nil {
				return err
			}
			headerReader.Shrink(int(record.MainDataLen))
		case XlrBlockIdOrigin:
			err := parsingutil.NewFieldToParse(&record.Origin, "origin").ParseFrom(headerReader)
			if err != nil {
				return err
			}
		default:
			blockHeader, err := readXLogRecordBlockHeader(lastRelFileNode, blockId, &maxReadBlockId, headerReader)
			if err != nil {
				return err
			}
			record.Blocks = append(record.Blocks, XLogRecordBlock{Header: *blockHeader})
		}
	}
	return nil
}

func readXLogRecordMainData(mainDataLen uint32, reader io.Reader) ([]byte, error) {
	mainData := make([]byte, mainDataLen)
	_, err := io.ReadFull(reader, mainData)
	return mainData, errors.WithStack(err)
}

func readXLogRecordBody(header *XLogRecordHeader, reader io.Reader) (*XLogRecord, error) {
	record := NewXLogRecord(*header)
	readXLogRecordBlockHeaderPart(record, reader)

	err := readXLogRecordBlockDataAndImages(record, reader)
	if err != nil {
		return nil, err
	}

	record.MainData, err = readXLogRecordMainData(record.MainDataLen, reader)
	if err != nil {
		return nil, err
	}
	return record, nil
}
