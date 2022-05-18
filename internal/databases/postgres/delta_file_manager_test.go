package postgres_test

import (
	"sync"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/testtools"
)

func TestGetCanceledDeltaFiles_MidWalFile(t *testing.T) {
	manager := postgres.NewDeltaFileManager(testtools.NewMockDataFolder())
	manager.CancelRecording(WalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := postgres.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)
}

func TestGetCanceledDeltaFiles_LastWalFile(t *testing.T) {
	manager := postgres.NewDeltaFileManager(testtools.NewMockDataFolder())
	manager.CancelRecording(LastWalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := postgres.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)

	nextWalFilename, err := postgres.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := postgres.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, nextDeltaFilename)
}

func TestGetBlockLocationConsumer_Exists(t *testing.T) {
	manager := postgres.NewDeltaFileManager(nil)
	deltaFileChanWriter := postgres.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, deltaFileChanWriter.BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := postgres.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, exists := manager.DeltaFileWriters.LoadExisting(DeltaFilename)
	assert.True(t, exists)
	assert.Equal(t, expectedConsumer.(*postgres.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(DeltaFilename)
	assert.NoError(t, err)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	err = deltaFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := postgres.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := postgres.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, exists := manager.DeltaFileWriters.LoadExisting(DeltaFilename)
	assert.True(t, exists)
	assert.Equal(t, deltaFile, expectedConsumer.(*postgres.DeltaFileChanWriter).DeltaFile)
	assert.Equal(t, expectedConsumer.(*postgres.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetPartFile_Exists(t *testing.T) {
	manager := postgres.NewDeltaFileManager(nil)
	expectedPartFile := postgres.NewWalPartFile()
	expectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename), expectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, expectedPartFile, actualPartFile)
}

func TestGetPartFile_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	notExpectedPartFile := postgres.NewWalPartFile()
	notExpectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename2), notExpectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	expectedPartFile, exists := manager.PartFiles.LoadExisting(postgres.ToPartFilename(DeltaFilename))
	assert.True(t, exists)
	assert.Equal(t, expectedPartFile.(*postgres.WalPartFile), actualPartFile)
}

func TestGetPartFile_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(postgres.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	partFile := postgres.NewWalPartFile()
	partFile.WalHeads[5] = []byte{2, 3, 123, 123, 1, 12}
	partFile.PreviousWalHead = []byte{222, 12, 32, 42, 52}
	err = partFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := postgres.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename2), postgres.NewWalPartFile())

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushPartFiles_CanceledFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename), postgres.NewWalPartFile())
	manager.CanceledDeltaFiles[DeltaFilename] = true
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)
	assert.True(t, dataFolder.IsEmpty())
}

func TestFlushPartFiles_CompleteFile(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	xLogRecord, xLogRecordData := testtools.GetXLogRecordData()
	for i := 0; i < int(postgres.WalFileInDelta); i++ {
		partFile.WalTails[i] = make([]byte, 0)
		partFile.WalHeads[i] = make([]byte, 0)
	}
	partFile.PreviousWalHead = xLogRecordData[:12]
	partFile.WalTails[0] = xLogRecordData[12:]

	manager := postgres.NewDeltaFileManager(nil)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFileWriter := postgres.NewDeltaFileChanWriter(deltaFile)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go deltaFileWriter.Consume(&waitGroup)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileWriter)
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename), partFile)
	completedPartFiles := manager.FlushPartFiles()
	assert.Contains(t, completedPartFiles, postgres.ToPartFilename(DeltaFilename))

	close(deltaFileWriter.BlockLocationConsumer)
	waitGroup.Wait()
	locations := walparser.ExtractBlockLocations([]walparser.XLogRecord{xLogRecord})
	assert.Equal(t, locations, deltaFileWriter.DeltaFile.Locations)
}

func TestFlushPartFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	partFile := postgres.NewWalPartFile()
	partFile.WalHeads[4] = []byte{1, 2, 3, 4}
	partFile.PreviousWalHead = []byte{5, 6, 7}
	partFile.WalTails[7] = []byte{8, 9, 123}
	manager.PartFiles.Store(postgres.ToPartFilename(DeltaFilename), partFile)
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)

	physicalPartFile, err := dataFolder.OpenReadonlyFile(postgres.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	actualPartFile, err := postgres.LoadPartFile(physicalPartFile)
	assert.NoError(t, err)

	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushDeltaFiles_CanceledFile(t *testing.T) {
	manager := postgres.NewDeltaFileManager(nil)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, postgres.NewDeltaFileChanWriter(deltaFile))
	manager.CanceledDeltaFiles[DeltaFilename] = true
	manager.FlushDeltaFiles(nil, map[string]bool{
		postgres.ToPartFilename(DeltaFilename): true,
	})
}

func TestFlushDeltaFiles_CompleteFile(t *testing.T) {
	manager := postgres.NewDeltaFileManager(nil)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, postgres.NewDeltaFileChanWriter(deltaFile))
	storage := memory.NewStorage()
	manager.FlushDeltaFiles(testtools.NewStoringMockUploader(storage, nil), map[string]bool{
		postgres.ToPartFilename(DeltaFilename): true,
	})

	actualDeltaFileData, ok := storage.Load("in_memory/" + DeltaFilename + ".mock")
	assert.True(t, ok)
	actualDeltaFile, err := postgres.LoadDeltaFile(&actualDeltaFileData.Data)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}

func TestFlushDeltaFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, postgres.NewDeltaFileChanWriter(deltaFile))
	manager.FlushDeltaFiles(nil, make(map[string]bool))

	actualDeltaFileReader, err := dataFolder.OpenReadonlyFile(DeltaFilename)
	assert.NoError(t, err)
	actualDeltaFile, err := postgres.LoadDeltaFile(actualDeltaFileReader)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}

func TestCombinePartFile(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	xLogRecord, xLogRecordData := testtools.GetXLogRecordData()
	for i := 0; i < int(postgres.WalFileInDelta); i++ {
		partFile.WalTails[i] = make([]byte, 0)
		partFile.WalHeads[i] = make([]byte, 0)
	}
	partFile.PreviousWalHead = xLogRecordData[:12]
	partFile.WalTails[0] = xLogRecordData[12:]

	manager := postgres.NewDeltaFileManager(nil)
	deltaFile, err := postgres.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFileWriter := postgres.NewDeltaFileChanWriter(deltaFile)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go deltaFileWriter.Consume(&waitGroup)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileWriter)
	err = manager.CombinePartFile(DeltaFilename, partFile)
	assert.NoError(t, err)

	close(deltaFileWriter.BlockLocationConsumer)
	waitGroup.Wait()
	locations := walparser.ExtractBlockLocations([]walparser.XLogRecord{xLogRecord})
	assert.Equal(t, locations, deltaFileWriter.DeltaFile.Locations)
}
