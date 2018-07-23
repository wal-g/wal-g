package walg

import (
	"github.com/wal-g/wal-g/walparser"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
)

// WalDeltaReader reads locations from delta files one by one
type WalDeltaReader struct {
	deltaFile       io.ReadCloser
	currentLocation walparser.BlockLocation
}

func NewWalDeltaReader(deltaFile io.ReadCloser) (*WalDeltaReader, error) {
	reader := WalDeltaReader{deltaFile: deltaFile}
	err := reader.readNextLocation()
	if err != nil {
		return nil, err
	}
	return &reader, err
}

func (reader *WalDeltaReader) readNextLocation() error {
	fields := []parsingutil.FieldToParse{
		{Field: &reader.currentLocation.RelationFileNode.SpcNode, Name: "SpcNode"},
		{Field: &reader.currentLocation.RelationFileNode.DBNode, Name: "DBNode"},
		{Field: &reader.currentLocation.RelationFileNode.RelNode, Name: "RelNode"},
		{Field: &reader.currentLocation.BlockNo, Name: "BlockNo"},
	}
	return parsingutil.ParseMultipleFieldsFromReader(fields, reader.deltaFile)
}
