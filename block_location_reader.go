package walg

import (
	"github.com/wal-g/wal-g/walparser"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
	"os"
)

type BlockLocationReader struct {
	deltaFile io.Reader
}

func (reader *BlockLocationReader) readNextLocation() (*walparser.BlockLocation, error) {
	var location walparser.BlockLocation
	fields := []parsingutil.FieldToParse{
		{Field: &location.RelationFileNode.SpcNode, Name: "SpcNode"},
		{Field: &location.RelationFileNode.DBNode, Name: "DBNode"},
		{Field: &location.RelationFileNode.RelNode, Name: "RelNode"},
		{Field: &location.BlockNo, Name: "BlockNo"},
	}
	err := parsingutil.ParseMultipleFieldsFromReader(fields, reader.deltaFile)
	if err != nil {
		return nil, err
	}
	return &location, nil
}

func (reader *BlockLocationReader) readAllLocations() ([]walparser.BlockLocation, error) {
	locations := make([]walparser.BlockLocation, 0)
	for {
		location, err := reader.readNextLocation()
		if err != nil {
			if err == io.EOF {
				return locations, nil
			}
			return locations, err
		}
		locations = append(locations, *location)
	}
}

func readAllLocationsFromFile(filename string) ([]walparser.BlockLocation, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := BlockLocationReader{file}
	return reader.readAllLocations()
}
