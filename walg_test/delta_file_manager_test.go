package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/walparser"
	"testing"
)

func TestGetCanceledDeltaFiles_MidWalFile(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	manager.CancelRecording(WalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := walg.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)
}

func TestGetCanceledDeltaFiles_LastWalFile(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	manager.CancelRecording(LastWalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := walg.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)

	nextWalFilename, err := walg.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := walg.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, nextDeltaFilename)
}

func TestGetBlockLocationConsumer_Exists(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	deltaFileChanWriter := walg.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, deltaFileChanWriter.BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := walg.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, ok := manager.DeltaFileWriters.Load(DeltaFilename)
	assert.True(t, ok)
	assert.Equal(t, expectedConsumer.(*walg.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(DeltaFilename)
	assert.NoError(t, err)
	deltaFile, err := walg.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	err = deltaFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := walg.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := walg.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, ok := manager.DeltaFileWriters.Load(DeltaFilename)
	assert.True(t, ok)
	assert.Equal(t, deltaFile, expectedConsumer.(*walg.DeltaFileChanWriter).DeltaFile)
	assert.Equal(t, expectedConsumer.(*walg.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetPartFile_Exists(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	expectedPartFile := walg.NewWalPartFile()
	expectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(walg.ToPartFilename(DeltaFilename), expectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, expectedPartFile, actualPartFile)
}

func TestGetPartFile_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)
	notExpectedPartFile := walg.NewWalPartFile()
	notExpectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(walg.ToPartFilename(DeltaFilename2), notExpectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	expectedPartFile, ok := manager.PartFiles.Load(walg.ToPartFilename(DeltaFilename))
	assert.True(t, ok)
	assert.Equal(t, expectedPartFile.(*walg.WalPartFile), actualPartFile)
}

func TestGetPartFile_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(walg.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	partFile := walg.NewWalPartFile()
	partFile.WalHeads[5] = []byte{2, 3, 123, 123, 1, 12}
	partFile.PreviousWalHead = []byte{222, 12, 32, 42, 52}
	err = partFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := walg.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(walg.ToPartFilename(DeltaFilename2), walg.NewWalPartFile())

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushPartFiles_CanceledFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(walg.ToPartFilename(DeltaFilename), walg.NewWalPartFile())
	manager.CanceledDeltaFiles[DeltaFilename] = true
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)
	assert.True(t, dataFolder.IsEmpty())
}

func TestFlushPartFiles_CompleteFile(t *testing.T) {
	// TODO
}

func TestFlushPartFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)
	partFile := walg.NewWalPartFile()
	partFile.WalHeads[4] = []byte{1, 2, 3, 4}
	partFile.PreviousWalHead = []byte{5, 6, 7}
	partFile.WalTails[7] = []byte{8, 9, 123}
	manager.PartFiles.Store(walg.ToPartFilename(DeltaFilename), partFile)
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)

	physicalPartFile, err := dataFolder.OpenReadonlyFile(walg.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	actualPartFile, err := walg.LoadPartFile(physicalPartFile)
	assert.NoError(t, err)

	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushDeltaFiles_CanceledFile(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	deltaFile, err := walg.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, walg.NewDeltaFileChanWriter(deltaFile))
	manager.CanceledDeltaFiles[DeltaFilename] = true
	manager.FlushDeltaFiles(nil, map[string]bool{
		walg.ToPartFilename(DeltaFilename): true,
	})
}

func TestFlushDeltaFiles_CompleteFile(t *testing.T) {
	manager := walg.NewDeltaFileManager(nil)
	deltaFile, err := walg.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, walg.NewDeltaFileChanWriter(deltaFile))
	storage := testtools.NewMockStorage()
	manager.FlushDeltaFiles(testtools.NewStoringMockTarUploader(false, false, storage), map[string]bool{
		walg.ToPartFilename(DeltaFilename): true,
	})

	actualDeltaFileData, ok := storage["bucket/server/wal_005/"+DeltaFilename+".mock"]
	assert.True(t, ok)
	actualDeltaFile, err := walg.LoadDeltaFile(&actualDeltaFileData)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}

func TestFlushDeltaFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)
	deltaFile, err := walg.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, walg.NewDeltaFileChanWriter(deltaFile))
	manager.FlushDeltaFiles(nil, make(map[string]bool))

	actualDeltaFileReader, err := dataFolder.OpenReadonlyFile(DeltaFilename)
	assert.NoError(t, err)
	actualDeltaFile, err := walg.LoadDeltaFile(actualDeltaFileReader)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}
