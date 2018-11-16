package walg

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"io"
)

type NilWalParserError struct {
	error
}

func NewNilWalParserError() NilWalParserError {
	return NilWalParserError{errors.New("expected to get non nil wal parser, but got nil one")}
}

type DeltaFile struct {
	Locations []walparser.BlockLocation
	WalParser *walparser.WalParser
}

func NewDeltaFile(walParser *walparser.WalParser) (*DeltaFile, error) {
	if walParser == nil {
		return nil, NewNilWalParserError()
	}
	return &DeltaFile{nil, walParser}, nil
}

func (deltaFile *DeltaFile) Save(writer io.Writer) error {
	err := WriteLocationsTo(writer, append(deltaFile.Locations, TerminalLocation))
	if err != nil {
		return err
	}
	return deltaFile.WalParser.Save(writer)
}

func LoadDeltaFile(reader io.Reader) (*DeltaFile, error) {
	locations, err := ReadLocationsFrom(reader)
	if err != nil {
		return nil, err
	}
	walParser, err := walparser.LoadWalParser(reader)
	if err != nil {
		return nil, err
	}
	return &DeltaFile{locations, walParser}, nil
}
