package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/internal/walparser"
	"sync"
	"testing"
)

func concatByteSlices(a []byte, b []byte) []byte {
	result := make([]byte, len(a)+len(b))
	copy(result, a)
	copy(result[len(a):], b)
	return result
}

func GetXLogRecordData() (walparser.XLogRecord, []byte) {
	imageData := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
	}
	blockData := []byte{
		0x0a, 0x0b, 0x0c,
	}
	mainData := []byte{
		0x0d, 0x0e, 0x0f, 0x10,
	}
	data := []byte{ // block header data
		0xfd, 0x01, 0xfe,
		0x00, 0x30, 0x03, 0x00, 0x0a, 0x00, 0xd4, 0x05, 0x05, 0x7f, 0x06, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x15, 0x40, 0x00, 0x00, 0xe4, 0x18, 0x00, 0x00,
		0xff, 0x04,
	}
	data = concatByteSlices(concatByteSlices(concatByteSlices(data, imageData), blockData), mainData)
	recordHeader := walparser.XLogRecordHeader{
		TotalRecordLength: uint32(walparser.XLogRecordHeaderSize + len(data)),
		XactID:            0x00000243,
		PrevRecordPtr:     0x000000002affedc8,
		Info:              0xb0,
		ResourceManagerID: 0x00,
		Crc32Hash:         0xecf5203c,
	}
	var recordHeaderData bytes.Buffer
	recordHeaderData.Write(internal.ToBytes(&recordHeader.TotalRecordLength))
	recordHeaderData.Write(internal.ToBytes(&recordHeader.XactID))
	recordHeaderData.Write(internal.ToBytes(&recordHeader.PrevRecordPtr))
	recordHeaderData.Write(internal.ToBytes(&recordHeader.Info))
	recordHeaderData.Write(internal.ToBytes(&recordHeader.ResourceManagerID))
	recordHeaderData.Write([]byte{0, 0})
	recordHeaderData.Write(internal.ToBytes(&recordHeader.Crc32Hash))
	recordData := concatByteSlices(recordHeaderData.Bytes(), data)
	record, _ := walparser.ParseXLogRecordFromBytes(recordData)
	return *record, recordData
}

func TestGetCanceledDeltaFiles_MidWalFile(t *testing.T) {
	manager := internal.NewDeltaFileManager(testtools.NewMockDataFolder())
	manager.CancelRecording(WalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := internal.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)
}

func TestGetCanceledDeltaFiles_LastWalFile(t *testing.T) {
	manager := internal.NewDeltaFileManager(testtools.NewMockDataFolder())
	manager.CancelRecording(LastWalFilename)
	manager.FlushFiles(nil)

	deltaFilename, err := internal.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, deltaFilename)

	nextWalFilename, err := internal.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := internal.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	assert.Contains(t, manager.CanceledDeltaFiles, nextDeltaFilename)
}

func TestGetBlockLocationConsumer_Exists(t *testing.T) {
	manager := internal.NewDeltaFileManager(nil)
	deltaFileChanWriter := internal.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, deltaFileChanWriter.BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := internal.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, exists := manager.DeltaFileWriters.LoadExisting(DeltaFilename)
	assert.True(t, exists)
	assert.Equal(t, expectedConsumer.(*internal.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetBlockLocationConsumer_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(DeltaFilename)
	assert.NoError(t, err)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	err = deltaFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := internal.NewDeltaFileManager(dataFolder)
	deltaFileChanWriter := internal.NewDeltaFileChanWriter(nil)
	manager.DeltaFileWriters.Store(DeltaFilename2, deltaFileChanWriter)

	consumer, err := manager.GetBlockLocationConsumer(DeltaFilename)
	assert.NoError(t, err)
	expectedConsumer, exists := manager.DeltaFileWriters.LoadExisting(DeltaFilename)
	assert.True(t, exists)
	assert.Equal(t, deltaFile, expectedConsumer.(*internal.DeltaFileChanWriter).DeltaFile)
	assert.Equal(t, expectedConsumer.(*internal.DeltaFileChanWriter).BlockLocationConsumer, consumer)
}

func TestGetPartFile_Exists(t *testing.T) {
	manager := internal.NewDeltaFileManager(nil)
	expectedPartFile := internal.NewWalPartFile()
	expectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename), expectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, expectedPartFile, actualPartFile)
}

func TestGetPartFile_CreateNew(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)
	notExpectedPartFile := internal.NewWalPartFile()
	notExpectedPartFile.WalHeads[3] = []byte{1, 2, 3, 4, 5}
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename2), notExpectedPartFile)

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	expectedPartFile, exists := manager.PartFiles.LoadExisting(internal.ToPartFilename(DeltaFilename))
	assert.True(t, exists)
	assert.Equal(t, expectedPartFile.(*internal.WalPartFile), actualPartFile)
}

func TestGetPartFile_Load(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	writer, err := dataFolder.OpenWriteOnlyFile(internal.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	partFile := internal.NewWalPartFile()
	partFile.WalHeads[5] = []byte{2, 3, 123, 123, 1, 12}
	partFile.PreviousWalHead = []byte{222, 12, 32, 42, 52}
	err = partFile.Save(writer)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	manager := internal.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename2), internal.NewWalPartFile())

	actualPartFile, err := manager.GetPartFile(DeltaFilename)
	assert.NoError(t, err)
	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushPartFiles_CanceledFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename), internal.NewWalPartFile())
	manager.CanceledDeltaFiles[DeltaFilename] = true
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)
	assert.True(t, dataFolder.IsEmpty())
}

func TestFlushPartFiles_CompleteFile(t *testing.T) {
	partFile := internal.NewWalPartFile()
	xLogRecord, xLogRecordData := GetXLogRecordData()
	for i := 0; i < int(internal.WalFileInDelta); i++ {
		partFile.WalTails[i] = make([]byte, 0)
		partFile.WalHeads[i] = make([]byte, 0)
	}
	partFile.PreviousWalHead = xLogRecordData[:12]
	partFile.WalTails[0] = xLogRecordData[12:]

	manager := internal.NewDeltaFileManager(nil)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFileWriter := internal.NewDeltaFileChanWriter(deltaFile)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go deltaFileWriter.Consume(&waitGroup)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileWriter)
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename), partFile)
	completedPartFiles := manager.FlushPartFiles()
	assert.Contains(t, completedPartFiles, internal.ToPartFilename(DeltaFilename))

	close(deltaFileWriter.BlockLocationConsumer)
	waitGroup.Wait()
	locations := internal.ExtractBlockLocations([]walparser.XLogRecord{xLogRecord})
	assert.Equal(t, locations, deltaFileWriter.DeltaFile.Locations)
}

func TestFlushPartFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)
	partFile := internal.NewWalPartFile()
	partFile.WalHeads[4] = []byte{1, 2, 3, 4}
	partFile.PreviousWalHead = []byte{5, 6, 7}
	partFile.WalTails[7] = []byte{8, 9, 123}
	manager.PartFiles.Store(internal.ToPartFilename(DeltaFilename), partFile)
	completedPartFiles := manager.FlushPartFiles()
	assert.Empty(t, completedPartFiles)

	physicalPartFile, err := dataFolder.OpenReadonlyFile(internal.ToPartFilename(DeltaFilename))
	assert.NoError(t, err)
	actualPartFile, err := internal.LoadPartFile(physicalPartFile)
	assert.NoError(t, err)

	assert.Equal(t, partFile, actualPartFile)
}

func TestFlushDeltaFiles_CanceledFile(t *testing.T) {
	manager := internal.NewDeltaFileManager(nil)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, internal.NewDeltaFileChanWriter(deltaFile))
	manager.CanceledDeltaFiles[DeltaFilename] = true
	manager.FlushDeltaFiles(nil, map[string]bool{
		internal.ToPartFilename(DeltaFilename): true,
	})
}

func TestFlushDeltaFiles_CompleteFile(t *testing.T) {
	manager := internal.NewDeltaFileManager(nil)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, internal.NewDeltaFileChanWriter(deltaFile))
	storage := testtools.NewInMemoryStorage()
	manager.FlushDeltaFiles(testtools.NewStoringMockUploader(storage, nil), map[string]bool{
		internal.ToPartFilename(DeltaFilename): true,
	})

	actualDeltaFileData, ok := storage.Load("in_memory/" + DeltaFilename + ".mock")
	assert.True(t, ok)
	actualDeltaFile, err := internal.LoadDeltaFile(&actualDeltaFileData.Data)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}

func TestFlushDeltaFiles_PartialFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	deltaFile.Locations = append(deltaFile.Locations, TestLocation)
	assert.NoError(t, err)
	manager.DeltaFileWriters.Store(DeltaFilename, internal.NewDeltaFileChanWriter(deltaFile))
	manager.FlushDeltaFiles(nil, make(map[string]bool))

	actualDeltaFileReader, err := dataFolder.OpenReadonlyFile(DeltaFilename)
	assert.NoError(t, err)
	actualDeltaFile, err := internal.LoadDeltaFile(actualDeltaFileReader)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, actualDeltaFile)
}

func TestCombinePartFile(t *testing.T) {
	partFile := internal.NewWalPartFile()
	xLogRecord, xLogRecordData := GetXLogRecordData()
	for i := 0; i < int(internal.WalFileInDelta); i++ {
		partFile.WalTails[i] = make([]byte, 0)
		partFile.WalHeads[i] = make([]byte, 0)
	}
	partFile.PreviousWalHead = xLogRecordData[:12]
	partFile.WalTails[0] = xLogRecordData[12:]

	manager := internal.NewDeltaFileManager(nil)
	deltaFile, err := internal.NewDeltaFile(walparser.NewWalParser())
	assert.NoError(t, err)
	deltaFileWriter := internal.NewDeltaFileChanWriter(deltaFile)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go deltaFileWriter.Consume(&waitGroup)
	manager.DeltaFileWriters.Store(DeltaFilename, deltaFileWriter)
	err = manager.CombinePartFile(DeltaFilename, partFile)
	assert.NoError(t, err)

	close(deltaFileWriter.BlockLocationConsumer)
	waitGroup.Wait()
	locations := internal.ExtractBlockLocations([]walparser.XLogRecord{xLogRecord})
	assert.Equal(t, locations, deltaFileWriter.DeltaFile.Locations)
}
