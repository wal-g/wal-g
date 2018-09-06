package walg

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
)

type BlockLocationReader struct {
	underlying io.Reader
}

func NewBlockLocationReader(underlying io.Reader) *BlockLocationReader {
	return &BlockLocationReader{underlying}
}

// ReadNextLocation returns any reader error wrapped with errors.Wrap
func (reader *BlockLocationReader) ReadNextLocation() (*walparser.BlockLocation, error) {
	var location walparser.BlockLocation
	fields := []parsingutil.FieldToParse{
		{Field: &location.RelationFileNode.SpcNode, Name: "SpcNode"},
		{Field: &location.RelationFileNode.DBNode, Name: "DBNode"},
		{Field: &location.RelationFileNode.RelNode, Name: "RelNode"},
		{Field: &location.BlockNo, Name: "BlockNo"},
	}
	err := parsingutil.ParseMultipleFieldsFromReader(fields, reader.underlying)
	if err != nil {
		return nil, err
	}
	return &location, nil
}

func ReadLocationsFrom(reader io.Reader) ([]walparser.BlockLocation, error) {
	locationReader := NewBlockLocationReader(reader)
	locations := make([]walparser.BlockLocation, 0)
	for {
		location, err := locationReader.ReadNextLocation()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return locations, nil
			}
			return locations, err
		}
		locations = append(locations, *location)
	}
}
