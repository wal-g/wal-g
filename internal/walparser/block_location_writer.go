package walparser

import (
	"encoding/binary"
	"io"
)

type BlockLocationWriter struct {
	Underlying io.Writer
}

func NewBlockLocationWriter(underlying io.Writer) *BlockLocationWriter {
	return &BlockLocationWriter{underlying}
}

func (locationWriter *BlockLocationWriter) WriteLocation(location BlockLocation) error {
	numbersToWrite := []uint32{
		uint32(location.RelationFileNode.SpcNode),
		uint32(location.RelationFileNode.DBNode),
		uint32(location.RelationFileNode.RelNode),
		location.BlockNo,
	}
	for _, number := range numbersToWrite {
		err := binary.Write(locationWriter.Underlying, binary.LittleEndian, number)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteLocationsTo(writer io.Writer, locations []BlockLocation) error {
	locationWriter := NewBlockLocationWriter(writer)
	for _, location := range locations {
		err := locationWriter.WriteLocation(location)
		if err != nil {
			return err
		}
	}
	return nil
}
