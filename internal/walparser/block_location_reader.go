package walparser

import (
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
)

type BlockLocationReader struct {
	underlying io.Reader
}

func NewBlockLocationReader(underlying io.Reader) *BlockLocationReader {
	return &BlockLocationReader{underlying}
}

// ReadNextLocation returns any reader error wrapped with errors.Wrap
func (reader *BlockLocationReader) ReadNextLocation() (*BlockLocation, error) {
	var location BlockLocation
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

func ReadLocationsFrom(reader io.Reader) ([]BlockLocation, error) {
	locationReader := NewBlockLocationReader(reader)
	locations := make([]BlockLocation, 0)
	for {
		location, err := locationReader.ReadNextLocation()
		if err != nil || *location == TerminalLocation {
			if errors.Cause(err) == io.EOF {
				err = nil
			}
			return locations, err
		}
		locations = append(locations, *location)
	}
}
