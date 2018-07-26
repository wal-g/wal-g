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
)

type WalDeltaRecordingReader struct {
	pageReader       walparser.WalPageReader
	walParser        walparser.WalParser
	pageDataLeftover []byte
	recorder         *WalDeltaRecorder // it can be nil and this indicates recording fail
}

func (reader *WalDeltaRecordingReader) Close() error {
	err := reader.saveParser()
	if err != nil {
		return err
	}
	if reader.recorder == nil {
		return nil
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
		err = reader.recordBlockNumbersFromRecords()
		if err != nil {
			reader.recorder.StopRecording(err)
			reader.recorder = nil
		}
	}
}

func (reader *WalDeltaRecordingReader) recordBlockNumbersFromRecords() error {
	if reader.recorder == nil {
		return nil
	}
	records, err := reader.walParser.ParseRecordsFromPage(bytes.NewReader(reader.pageDataLeftover))
	if err != nil && err != walparser.PartialPageError {
		if err == walparser.ZeroPageError {
			return nil
		}
		reader.walParser.Invalidate()
		return err
	}
	return reader.recorder.recordWalDelta(records)
}

func NewWalDeltaRecordingReader(walFileReader io.Reader, walFilename string, s3Prefix *S3Folder, uploader *Uploader) (*WalDeltaRecordingReader, error) {
	walParser, recorder, err := tryOpenParserAndRecorder(walFilename, s3Prefix, uploader)
	if err != nil {
		deltaFileName, err1 := getDeltaFileNameFor(walFilename)
		if err1 != nil {
			os.Remove(deltaFileName)
		}
		return nil, err
	}
	return &WalDeltaRecordingReader{
		*walparser.NewWalPageReader(walFileReader),
		*walParser,
		nil,
		recorder,
	}, nil
}

func tryOpenParserAndRecorder(walFilename string, s3Prefix *S3Folder, uploader *Uploader) (*walparser.WalParser, *WalDeltaRecorder, error) {
	walParser, err := loadWalParser()
	if err != nil {
		return nil, nil, err
	}
	recorder, err := NewWalDeltaRecorder(walFilename, s3Prefix, uploader)
	if err != nil {
		return nil, nil, err
	}
	return walParser, recorder, nil
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
