package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/walparser"
	"os"
	"testing"
)

const (
	WalgTestDataFolderPath = "./testdata"
	WalFilename            = "00000001000000000000007C"
	DeltaFilename          = "000000010000000000000070_delta"
)

var TestLocation = *walparser.NewBlockLocation(1, 2, 3, 4)

func TestGetDeltaFileNameFor(t *testing.T) {
	deltaFilename, err := walg.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	assert.Equal(t, DeltaFilename, deltaFilename)
}

func TestOpenDeltaFileFor_NewDeltaFile(t *testing.T) {
	walFilename := "000000010000000000000070"
	file, err := walg.OpenDeltaFileFor(WalgTestDataFolderPath, walFilename)
	assert.NoError(t, err)
	assert.Equal(t, DeltaFilePath, file.Name())
	file.Close()
	os.Remove(DeltaFilePath)
}

func TestOpenDeltaFileFor_ExistingDeltaFile(t *testing.T) {
	file, err := os.Create(DeltaFilePath)
	assert.NoError(t, err)
	err = file.Close()
	assert.NoError(t, err)
	file, err = walg.OpenDeltaFileFor(WalgTestDataFolderPath, WalFilename)
	assert.NoError(t, err)
	assert.Equal(t, DeltaFilePath, file.Name())
	file.Close()
	os.Remove(DeltaFilePath)
}

func TestOpenDeltaFileFor_NoDeltaFile(t *testing.T) {
	walFilename := "000000040000000000000075"
	_, err := walg.OpenDeltaFileFor(WalgTestDataFolderPath, walFilename)
	assert.Error(t, err)
}

func TestSendDeltaToS3(t *testing.T) {
	storage := testtools.NewMockStorage()
	recorder, err := walg.NewWalDeltaRecorder(
		WalgTestDataFolderPath,
		"000000010000000000000070",
		testtools.NewStoringMockTarUploader(false, false, storage),
	)
	defer os.Remove(recorder.DeltaFile.Name())
	defer recorder.DeltaFile.Close()
	assert.NoError(t, err)
	err = recorder.SendDeltaToS3([]walparser.BlockLocation{TestLocation})
	assert.NoError(t, err)
	assertContainsTestLocation(t, storage)
}

func TestClose_NoSending(t *testing.T) {
	recorder, err := walg.NewWalDeltaRecorder(WalgTestDataFolderPath, "000000010000000000000070", nil)
	defer os.Remove(DeltaFilePath)
	assert.NoError(t, err)
	err = recorder.Close()
	assert.NoError(t, err)
}

func TestClose_Sending(t *testing.T) {
	storage := testtools.NewMockStorage()
	deltaFile, err := os.Create(DeltaFilePath)
	assert.NoError(t, err)
	locationWriter := walg.NewBlockLocationWriter(deltaFile)
	err = locationWriter.WriteLocation(TestLocation)
	assert.NoError(t, err)
	recorder, err := walg.NewWalDeltaRecorder(
		WalgTestDataFolderPath,
		"00000001000000000000007F",
		testtools.NewStoringMockTarUploader(false, false, storage),
	)
	assert.NoError(t, err)
	err = recorder.Close()
	assert.NoError(t, err)
	assertContainsTestLocation(t, storage)
}

func assertContainsTestLocation(t *testing.T, storage testtools.MockStorage) {
	storageDeltaFilePath := "bucket/server/wal_005/000000010000000000000070_delta.mock"
	locationBuffer := storage[storageDeltaFilePath]
	reader := walg.NewBlockLocationReader(&locationBuffer)
	location, err := reader.ReadNextLocation()
	assert.NoError(t, err)
	assert.NotNil(t, location)
	assert.Equal(t, TestLocation, *location)
}
