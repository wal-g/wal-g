package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"io"
	"os"
	"path"
)

const (
	PathToDataFolder   = "~/walg_data"
	RecordPartFilename = "currentRecort.part"
	DeltaFilename      = "wal_delta"
	OldDeltaFilename   = "wal_delta_old"
)

type WalDeltaRecordingReader struct {
	pageReader       walparser.WalPageReader
	walParser        walparser.WalParser
	pageDataLeftover []byte
	recorder         WalDeltaRecorder
}

func (reader *WalDeltaRecordingReader) Close() error {
	err := reader.saveParser()
	if err != nil {
		return err
	}
	return reader.recorder.Close()
}

func (reader *WalDeltaRecordingReader) saveParser() error {
	file, err := os.Open(path.Join(PathToDataFolder, RecordPartFilename))
	if err != nil {
		return err
	}
	defer file.Close()
	return reader.walParser.SaveParser(file)
}

func (reader *WalDeltaRecordingReader) Read(p []byte) (n int, err error) {
	dataExpected := len(p)
	for {
		if dataExpected <= len(reader.pageDataLeftover) {
			copy(p, reader.pageDataLeftover[:dataExpected])
			reader.pageDataLeftover = reader.pageDataLeftover[dataExpected:]
			return len(p), nil
		}
		copy(p, reader.pageDataLeftover)
		dataExpected -= len(reader.pageDataLeftover)
		reader.pageDataLeftover, err = reader.pageReader.ReadPageData()
		if err != nil && (err != io.EOF || len(reader.pageDataLeftover) != int(WalPageSize)) {
			return len(p) - dataExpected, err
		}
		err = reader.extractBlockNumbersFromRecords()
		if err != nil { // TODO : what to do with errors from recorder?
			return len(p) - dataExpected, err
		}
	}
}

func (reader *WalDeltaRecordingReader) extractBlockNumbersFromRecords() error {
	records, err := reader.walParser.ParseRecordsFromPage(bytes.NewReader(reader.pageDataLeftover))
	if err != nil && err != walparser.PartialPageError {
		if err == walparser.ZeroPageError {
			return nil
		}
		return err
	}
	return reader.recorder.recordWalDelta(records)
}

func NewWalDeltaRecordingReader(walFileReader io.Reader) (*WalDeltaRecordingReader, error) {
	walParser, err := loadWalParser()
	if err != nil {
		return nil, err
	}
	recorder, err := NewWalDeltaRecorder()
	if err != nil {
		return nil, err
	}
	return &WalDeltaRecordingReader{
		*walparser.NewWalPageReader(walFileReader),
		*walParser,
		nil,
		*recorder,
	}, nil
}

func loadWalParser() (*walparser.WalParser, error) {
	pathToParser := path.Join(PathToDataFolder, RecordPartFilename)
	parserFile, err := os.Open(pathToParser)
	if err != nil {
		if os.IsNotExist(err) {
			return walparser.NewWalParser(), nil
		}
		return nil, err
	}
	parser, err := walparser.LoadParser(parserFile)
	if err != nil {
		return nil, err
	}
	return parser, nil
}
