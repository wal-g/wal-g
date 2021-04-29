package postgres_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

var ParserFilePath = path.Join(WalgTestDataFolderPath, postgres.RecordPartFilename)
var WalFilePath = path.Join(WalgTestDataFolderPath, WalFilename)
var DeltaFilePath = path.Join(WalgTestDataFolderPath, DeltaFilename)
var RealLocation = *walparser.NewBlockLocation(postgres.DefaultSpcNode, 16384, 16397, 2062)

func TestRecordBlockLocationsFromPage(t *testing.T) {
	walParser := walparser.NewWalParser()
	walFile, err := os.Open(WalFilePath)
	assert.NoError(t, err)
	pageReader := walparser.NewWalPageReader(walFile)
	page1, err := pageReader.ReadPageData()
	assert.NoError(t, err)
	page2, err := pageReader.ReadPageData()
	assert.NoError(t, err)

	_, _, err = walParser.ParseRecordsFromPage(bytes.NewReader(page1)) // initializing
	assert.NoError(t, err)

	blockLocationConsumer := make(chan walparser.BlockLocation)
	recordingReader := postgres.WalDeltaRecordingReader{
		WalParser:        *walParser,
		PageDataLeftover: page2,
		Recorder:         postgres.NewWalDeltaRecorder(blockLocationConsumer),
	}
	go func() {
		err = recordingReader.RecordBlockLocationsFromPage()
		assert.NoError(t, err)

		close(blockLocationConsumer)
	}()
	locations := make([]walparser.BlockLocation, 0)
	for location := range blockLocationConsumer {
		locations = append(locations, location)
	}
	assert.Len(t, locations, 1)
	assert.Equal(t, RealLocation, locations[0])
}

func TestRead_CorrectData(t *testing.T) {
	data, err := ioutil.ReadFile(WalFilePath)
	assert.NoError(t, err)
	reader := postgres.WalDeltaRecordingReader{
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
	defer utility.LoggedClose(walFile, "")

	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	recordingReader, err := postgres.NewWalDeltaRecordingReader(walFile, WalFilename, manager)
	assert.NoError(t, err)

	_, err = ioutil.ReadAll(recordingReader)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	locations, err := walparser.ReadLocationsFrom((*dataFolder)[DeltaFilename])
	assert.NoError(t, err)
	assert.Len(t, locations, 1)
	assert.Equal(t, RealLocation, locations[0])
}

func TestRead_RecordingFail(t *testing.T) {
	walData := make([]byte, walparser.WalPageSize*3)
	for i := range walData {
		walData[i] = 1
	}

	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	recordingReader, err := postgres.NewWalDeltaRecordingReader(bytes.NewReader(walData), WalFilename, manager)
	assert.NoError(t, err)

	actualData, err := ioutil.ReadAll(recordingReader)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	assert.Equal(t, walData, actualData)
	assert.True(t, dataFolder.IsEmpty())
}
