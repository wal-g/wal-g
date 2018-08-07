package walg_test

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/walparser"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

var ParserFilePath = path.Join(WalgTestDataFolderPath, walg.RecordPartFilename)
var WalFilePath = path.Join(WalgTestDataFolderPath, WalFilename)
var DeltaFilePath = path.Join(WalgTestDataFolderPath, DeltaFilename)
var RealLocation = *walparser.NewBlockLocation(walg.DefaultSpcNode, 16384, 16397, 2062)

func createWalPageWithContinuation() []byte {
	pageHeader := walparser.XLogPageHeader{
		Info:             walparser.XlpFirstIsContRecord,
		RemainingDataLen: 12312,
	}
	data := make([]byte, 20)
	binary.LittleEndian.PutUint16(data, pageHeader.Magic)
	binary.LittleEndian.PutUint16(data, pageHeader.Info)
	binary.LittleEndian.PutUint32(data, uint32(pageHeader.TimeLineID))
	binary.LittleEndian.PutUint64(data, uint64(pageHeader.PageAddress))
	binary.LittleEndian.PutUint32(data, pageHeader.RemainingDataLen)
	for len(data) < int(walparser.WalPageSize) {
		data = append(data, 2)
	}
	return data
}

func createWalParser() (*walparser.WalParser, error) {
	data := createWalPageWithContinuation()

	walParser := walparser.NewWalParser()
	_, err := walParser.ParseRecordsFromPage(bytes.NewReader(data)) // initializing parsing
	if err != nil {
		return nil, err
	}
	return walParser, nil
}

func TestSaveParser(t *testing.T) {
	walParser, err := createWalParser()
	assert.NoError(t, err)
	recordingReader := walg.WalDeltaRecordingReader{
		WalParser:      *walParser,
		DataFolderPath: WalgTestDataFolderPath,
	}
	err = recordingReader.SaveParser()
	assert.NoError(t, err)

	parserFile, err := os.Open(ParserFilePath)
	assert.NoError(t, err)
	actualParser, err := walparser.LoadParser(parserFile)
	assert.NoError(t, err)
	parserFile.Close()
	assert.Equal(t, walParser, actualParser)
	os.Remove(ParserFilePath)
}

func TestLoadWalParser(t *testing.T) {
	walParser, err := createWalParser()
	assert.NoError(t, err)
	parserFile, err := os.Create(ParserFilePath)
	assert.NoError(t, err)
	walParser.SaveParser(parserFile)
	parserFile.Close()

	actualParser, err := walg.LoadWalParser(WalgTestDataFolderPath)
	assert.NoError(t, err)
	assert.Equal(t, walParser, actualParser)
	os.Remove(ParserFilePath)
}

func TestRecordBlockLocationsFromPage(t *testing.T) {
	deltaFile, err := os.Create(DeltaFilePath)
	assert.NoError(t, err)
	defer os.Remove(DeltaFilePath)
	defer deltaFile.Close()

	walParser := walparser.NewWalParser()
	walFile, err := os.Open(WalFilePath)
	assert.NoError(t, err)
	pageReader := walparser.NewWalPageReader(walFile)
	page1, err := pageReader.ReadPageData()
	assert.NoError(t, err)
	page2, err := pageReader.ReadPageData()
	assert.NoError(t, err)

	_, err = walParser.ParseRecordsFromPage(bytes.NewReader(page1)) // initializing
	assert.NoError(t, err)

	recordingReader := walg.WalDeltaRecordingReader{
		WalParser:        *walParser,
		PageDataLeftover: page2,
		Recorder:         &walg.WalDeltaRecorder{DeltaFile: deltaFile},
	}
	err = recordingReader.RecordBlockLocationsFromPage()
	assert.NoError(t, err)

	_, err = deltaFile.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	locations, err := walg.ReadLocationsFrom(deltaFile)
	assert.NoError(t, err)
	assert.Len(t, locations, 1)
	assert.Equal(t, RealLocation, locations[0])
}

func TestRead_CorrectData(t *testing.T) {
	data, err := ioutil.ReadFile(WalFilePath)
	assert.NoError(t, err)
	reader := walg.WalDeltaRecordingReader{
		PageReader: *walparser.NewWalPageReader(bytes.NewReader(data)),
	}
	actualData := make([]byte, len(data), cap(data))
	n, err := reader.Read(actualData)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, actualData)
}

func TestRead_CorrectRecording(t *testing.T) {
	walFile, err := os.Open(WalFilePath)
	assert.NoError(t, err)
	defer walFile.Close()

	file, err := os.Create(DeltaFilePath)
	assert.NoError(t, err)
	file.Close()
	defer os.Remove(DeltaFilePath)

	recordingReader, err := walg.NewWalDeltaRecordingReader(walFile, WalFilename, nil, WalgTestDataFolderPath)
	assert.NoError(t, err)
	defer recordingReader.Recorder.DeltaFile.Close()

	_, err = ioutil.ReadAll(recordingReader)
	assert.NoError(t, err)

	_, err = recordingReader.Recorder.DeltaFile.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	locations, err := walg.ReadLocationsFrom(recordingReader.Recorder.DeltaFile)
	assert.NoError(t, err)
	assert.Len(t, locations, 1)
	assert.Equal(t, RealLocation, locations[0])
}

func TestRead_RecordingFail(t *testing.T) {
	deltaFile, err := os.Create(DeltaFilePath)
	assert.NoError(t, err)
	err = deltaFile.Close()
	assert.NoError(t, err)

	walData := make([]byte, walparser.WalPageSize*3)
	for i := range walData {
		walData[i] = 1
	}

	recordingReader, err := walg.NewWalDeltaRecordingReader(bytes.NewReader(walData), WalFilename, nil, WalgTestDataFolderPath)
	assert.NoError(t, err)

	actualData, err := ioutil.ReadAll(recordingReader)
	assert.NoError(t, err)
	assert.Equal(t, walData, actualData)
	_, err = os.Stat(DeltaFilePath)
	assert.True(t, os.IsNotExist(err))
}
