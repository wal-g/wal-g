package walg_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/walparser"
	"os"
	"testing"
	"time"
)

var BundleTestLocations = []walparser.BlockLocation{
	*walparser.NewBlockLocation(1, 2, 3, 4),
	*walparser.NewBlockLocation(5, 6, 7, 8),
	*walparser.NewBlockLocation(1, 2, 3, 9),
}

func TestEmptyBundleQueue(t *testing.T) {

	bundle := &walg.Bundle{
		TarSizeThreshold: 100,
	}

	uploader := testtools.NewMockTarUploader(false, false)

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "",
		BackupName:       "mockBackup",
		Uploader:         uploader,
	}

	bundle.StartQueue()

	err := bundle.FinishQueue()
	if err != nil {
		t.Log(err)
	}
}

func TestBundleQueue(t *testing.T) {
	queueTest(t)
}

func TestBundleQueueHighConcurrency(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "100")

	queueTest(t)

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestBundleQueueLowConcurrency(t *testing.T) {
	os.Setenv("WALG_UPLOAD_CONCURRENCY", "1")

	queueTest(t)

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func queueTest(t *testing.T) {
	bundle := &walg.Bundle{
		TarSizeThreshold: 100,
	}
	uploader := testtools.NewMockTarUploader(false, false)
	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "",
		BackupName:       "mockBackup",
		Uploader:         uploader,
	}

	// For tests there must be at least 3 workers

	bundle.StartQueue()

	a := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		bundle.EnqueueBack(a)
	}()

	c := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bundle.CheckSizeAndEnqueueBack(c)
	}()

	b := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bundle.EnqueueBack(b)
	}()

	err := bundle.FinishQueue()
	if err != nil {
		t.Log(err)
	}
}

func makeDeltaFile(locations []walparser.BlockLocation) ([]byte, error) {
	var data bytes.Buffer
	compressor := walg.Compressors[walg.Lz4AlgorithmName]
	err := walg.WriteLocationsTo(compressor.NewWriter(&data), locations)
	if err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func putDeltaIntoStorage(storage testtools.MockStorage, locations []walparser.BlockLocation, deltaFilename string) error {
	deltaData, err := makeDeltaFile(locations)
	if err != nil {
		return err
	}
	storage["mock/mock/wal_005/"+deltaFilename+".lz4"] = *bytes.NewBuffer(deltaData)
	return nil
}

func putWalIntoStorage(storage testtools.MockStorage, data []byte, walFilename string) error {
	compressor := walg.Compressors[walg.Lz4AlgorithmName]
	var compressedData bytes.Buffer
	compressingWriter := compressor.NewWriter(&compressedData)
	_, err := compressingWriter.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return err
	}
	storage["mock/mock/wal_005/"+walFilename+".lz4"] = compressedData
	return nil
}

func fillStorageWithMockDeltas(storage testtools.MockStorage) error {
	err := putDeltaIntoStorage(
		storage,
		[]walparser.BlockLocation{
			BundleTestLocations[0],
			BundleTestLocations[1],
		},
		"000000010000000000000070_delta",
	)
	if err != nil {
		return err
	}
	err = putDeltaIntoStorage(
		storage,
		[]walparser.BlockLocation{
			BundleTestLocations[0],
			BundleTestLocations[2],
		},
		"000000010000000000000080_delta",
	)
	if err != nil {
		return err
	}
	err = putDeltaIntoStorage(
		storage,
		[]walparser.BlockLocation{
			BundleTestLocations[2],
		},
		"0000000100000000000000A0_delta",
	)
	if err != nil {
		return err
	}
	err = putWalIntoStorage(storage, createWalPageWithContinuation(), "000000010000000000000090")
	return err
}

func setupFolderAndBundle() (folder *walg.S3Folder, bundle *walg.Bundle, err error) {
	storage := testtools.NewMockStorage()
	err = fillStorageWithMockDeltas(storage)
	if err != nil {
		return nil, nil, err
	}
	folder = walg.NewS3Folder(testtools.NewMockStoringS3Client(storage), "mock/", "mock")
	currentBackupFirstWalFilename := "000000010000000000000073"
	timeLine, logSegNo, err := walg.ParseWALFileName(currentBackupFirstWalFilename)
	if err != nil {
		return nil, nil, err
	}
	logSegNo *= walg.WalSegmentSize
	bundle = &walg.Bundle{
		Timeline:         timeLine,
		IncrementFromLsn: &logSegNo,
	}
	return
}

func TestLoadDeltaMap_AllDeltas(t *testing.T) {
	folder, bundle, err := setupFolderAndBundle()
	assert.NoError(t, err)

	backupNextWalFilename := "000000010000000000000090"
	_, curLogSegNo, err := walg.ParseWALFileName(backupNextWalFilename)

	err = bundle.DownloadDeltaMap(folder, curLogSegNo*walg.WalSegmentSize)
	assert.NoError(t, err)
	assert.NotNil(t, bundle.DeltaMap)
	assert.Equal(t, []uint32{4, 9}, bundle.DeltaMap[BundleTestLocations[0].RelationFileNode].ToArray())
	assert.Equal(t, []uint32{8}, bundle.DeltaMap[BundleTestLocations[1].RelationFileNode].ToArray())
}

func TestLoadDeltaMap_MissingDelta(t *testing.T) {
	folder, bundle, err := setupFolderAndBundle()
	assert.NoError(t, err)

	backupNextWalFilename := "0000000100000000000000B0"
	_, curLogSegNo, err := walg.ParseWALFileName(backupNextWalFilename)

	err = bundle.DownloadDeltaMap(folder, curLogSegNo*walg.WalSegmentSize)
	assert.Error(t, err)
	assert.Nil(t, bundle.DeltaMap)
}

func TestLoadDeltaMap_WalTail(t *testing.T) {
	folder, bundle, err := setupFolderAndBundle()
	assert.NoError(t, err)

	backupNextWalFilename := "000000010000000000000091"
	_, curLogSegNo, err := walg.ParseWALFileName(backupNextWalFilename)

	err = bundle.DownloadDeltaMap(folder, curLogSegNo*walg.WalSegmentSize)
	assert.NoError(t, err)
	assert.NotNil(t, bundle.DeltaMap)
	assert.Equal(t, []uint32{4, 9}, bundle.DeltaMap[BundleTestLocations[0].RelationFileNode].ToArray())
	assert.Equal(t, []uint32{8}, bundle.DeltaMap[BundleTestLocations[1].RelationFileNode].ToArray())
}
