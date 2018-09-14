package walg

import (
	"github.com/wal-g/wal-g/walparser"
	"io"
)

type DeltaFile struct {
	locations []walparser.BlockLocation
	walParser *walparser.WalParser
}

func NewDeltaFile(walParser *walparser.WalParser) *DeltaFile {
	return &DeltaFile{nil, walParser}
}

// TODO : unit tests
func (deltaFile *DeltaFile) save(writer io.Writer) error {
	err := WriteLocationsTo(writer, append(deltaFile.locations, TerminalLocation))
	if err != nil {
		return err
	}
	return deltaFile.walParser.SaveParser(writer)
}

// TODO : unit tests
func loadDeltaFile(reader io.Reader) (*DeltaFile, error) {
	locations, err := ReadLocationsFrom(reader)
	if err != nil {
		return nil, err
	}
	walParser, err := walparser.LoadParser(reader)
	if err != nil {
		return nil, err
	}
	return &DeltaFile{locations, walParser}, nil
}
