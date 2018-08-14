package walg

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser"
	"io"
	"os"
	"path"
)

const (
	RecordPartFilename = "currentRecord.part"
)

// In case of recording error WalDeltaRecordingReader stops recording, but continues reading data correctly
type WalDeltaRecordingReader struct {
	PageReader       walparser.WalPageReader
	WalParser        walparser.WalParser
	PageDataLeftover []byte
	Recorder         *WalDeltaRecorder // it can be nil and this indicates recording fail
	DataFolderPath   string
}

func (reader *WalDeltaRecordingReader) Close() error {
	err := reader.SaveParser()
	if err != nil {
		return err
	}
	if reader.Recorder == nil {
		return nil
	}
	return reader.Recorder.Close()
}

func (reader *WalDeltaRecordingReader) SaveParser() error {
	parserFilename := path.Join(reader.DataFolderPath, RecordPartFilename)
	file, err := os.Create(parserFilename)
	if err != nil {
		return err
	}
	defer file.Close()
	return reader.WalParser.SaveParser(file)
}

func (reader *WalDeltaRecordingReader) Read(p []byte) (n int, err error) {
	dataExpected := len(p)
	for {
		if len(p) <= len(reader.PageDataLeftover) {
			copy(p, reader.PageDataLeftover[:len(p)])
			reader.PageDataLeftover = reader.PageDataLeftover[len(p):]
			return dataExpected, nil
		}
		copy(p, reader.PageDataLeftover)
		p = p[len(reader.PageDataLeftover):]
		reader.PageDataLeftover, err = reader.PageReader.ReadPageData()
		if err != nil {
			if err != io.EOF && reader.Recorder != nil {
				reader.Recorder.stopRecording(err)
				reader.Recorder = nil
			}
			return dataExpected - len(p), err
		}
		recordingErr := reader.RecordBlockLocationsFromPage()
		if recordingErr != nil {
			reader.Recorder.stopRecording(recordingErr)
			reader.Recorder = nil
		}
	}
}

func (reader *WalDeltaRecordingReader) RecordBlockLocationsFromPage() error {
	if reader.Recorder == nil {
		return nil
	}
	records, err := reader.WalParser.ParseRecordsFromPage(bytes.NewReader(reader.PageDataLeftover))
	if err != nil && err != walparser.PartialPageError {
		if err == walparser.ZeroPageError {
			return nil
		}
		reader.WalParser.Invalidate()
		return err
	}
	return reader.Recorder.recordWalDelta(records)
}

func NewWalDeltaRecordingReader(walFileReader io.Reader, walFilename string, uploader *Uploader, dataFolderPath string) (*WalDeltaRecordingReader, error) {
	_, err := os.Stat(dataFolderPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataFolderPath, os.ModePerm)
	}
	if err != nil {
		return nil, err
	}
	walParser, recorder, err := tryOpenParserAndRecorder(dataFolderPath, walFilename, uploader)
	if err != nil {
		deltaFileName, err1 := GetDeltaFilenameFor(walFilename)
		if err1 == nil {
			os.Remove(deltaFileName)
		}
		return nil, err
	}
	return &WalDeltaRecordingReader{
		*walparser.NewWalPageReader(walFileReader),
		*walParser,
		nil,
		recorder,
		dataFolderPath,
	}, nil
}

func tryOpenParserAndRecorder(dataFolderPath, walFilename string, uploader *Uploader) (*walparser.WalParser, *WalDeltaRecorder, error) {
	walParser, err := LoadWalParser(dataFolderPath)
	if err != nil {
		return nil, nil, err
	}
	recorder, err := NewWalDeltaRecorder(dataFolderPath, walFilename, uploader)
	if err != nil {
		return nil, nil, err
	}
	return walParser, recorder, nil
}

func LoadWalParser(dataFolderPath string) (*walparser.WalParser, error) {
	pathToParser := path.Join(dataFolderPath, RecordPartFilename)
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
