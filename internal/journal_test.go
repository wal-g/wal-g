package internal_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

var (
	JournalFmt           = "%09d"
	SentinelName         = "sentinel"
	SentinelS3Name       = internal.JournalPrefix + "sentinel"
	MinimalJournalNumber = "000000000"
	MaximalJournalNumber = "999999999"
)

func GenerateDataAndTest(
	t *testing.T,
	recordCount, recordSize int,
	beginJournalID, endJournalID string,
	expectedSum int64,
) {
	GenerateS3SetAndCheckJournalSizes(t, recordCount, recordSize, beginJournalID, endJournalID, expectedSum)
	GenerateS3SetAndUpdateBackupsInfo(t, recordCount, recordSize, beginJournalID, endJournalID, expectedSum)
	GenerateS3SetAndUpdateBackupsInfoManyTimes(t, recordCount, recordSize, beginJournalID, endJournalID, expectedSum)
	GenerateS3AndUpdateLastBackup(t, recordCount, recordSize, beginJournalID, endJournalID, expectedSum)
}

func GenerateS3SetAndCheckJournalSizes(
	t *testing.T,
	recordCount, recordSize int,
	beginJournalID, endJournalID string,
	expectedSum int64,
) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	journalSize, err := internal.GetJournalSizeInSemiInterval(
		root,
		utility.WalPath,
		func(a, b string) bool {
			return a < b
		},
		beginJournalID,
		endJournalID,
	)
	assert.NoError(t, err)
	assert.Equal(t, expectedSum, journalSize)
}

func GenerateS3SetAndUpdateBackupsInfo(
	t *testing.T,
	recordCount, recordSize int,
	beginJournalID, endJournalID string,
	expectedSum int64,
) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	backupInfo, err := internal.GetBackupInfo(root, SentinelS3Name)
	assert.Error(t, err)
	assert.Equal(t, backupInfo, internal.JournalInfo{})

	createEmptySentinel(t, root)

	backupInfo, err = internal.GetBackupInfo(root, SentinelS3Name)
	assert.NoError(t, err)
	assert.Equal(t, backupInfo, internal.JournalInfo{})

	backupInfo.JournalSize, err = internal.GetJournalSizeInSemiInterval(
		root,
		utility.WalPath,
		func(a, b string) bool {
			return a < b
		},
		beginJournalID,
		endJournalID,
	)
	assert.Equal(t, backupInfo, internal.JournalInfo{
		JournalSize: expectedSum,
	})
	assert.NoError(t, err)

	err = internal.UploadBackupInfo(root, SentinelName, backupInfo)
	assert.NoError(t, err)

	backupInfo, err = internal.GetBackupInfo(root, SentinelS3Name)
	assert.NoError(t, err)
	assert.Equal(t, backupInfo, internal.JournalInfo{
		JournalSize: expectedSum,
	})
}

func GenerateS3AndUpdateLastBackup(
	t *testing.T,
	recordCount, recordSize int,
	beginJournalID, endJournalID string,
	expectedSum int64,
) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	createEmptySentinel(t, root)

	backupInfo, err := internal.GetBackupInfo(root, SentinelS3Name)
	assert.NoError(t, err)
	assert.Equal(t, backupInfo, internal.JournalInfo{})

	err = internal.UploadBackupInfo(root, SentinelName, internal.JournalInfo{
		JournalStart: beginJournalID,
		JournalEnd:   beginJournalID,
		JournalSize:  0,
	})
	assert.NoError(t, err)

	err = internal.UpdatePreviousBackupInfo(
		root,
		utility.WalPath,
		func(a, b string) bool { return a < b },
		endJournalID,
	)
	assert.NoError(t, err)

	backupInfo, err = internal.GetBackupInfo(root, SentinelS3Name)
	assert.NoError(t, err)

	assert.Equal(t, backupInfo.JournalSize, expectedSum)
}

func GenerateS3SetAndUpdateBackupsInfoManyTimes(
	t *testing.T,
	recordCount, recordSize int,
	beginJournalID, endJournalID string,
	expectedSum int64,
) {
	times := 10
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	for i := 0; i < times; i++ {
		var backupInfo internal.JournalInfo
		var err error

		backupInfo.JournalSize, err = internal.GetJournalSizeInSemiInterval(
			root,
			utility.WalPath,
			func(a, b string) bool {
				return a < b
			},
			beginJournalID,
			endJournalID,
		)
		assert.NoError(t, err)

		sentinel := fmt.Sprintf("%s%d", SentinelName, i)
		err = internal.UploadBackupInfo(root, sentinel, backupInfo)
		assert.NoError(t, err)

		sentinel = fmt.Sprintf("%s%d", SentinelS3Name, i)
		backupInfo, err = internal.GetBackupInfo(root, sentinel)
		assert.NoError(t, err)
		assert.Equal(t, backupInfo, internal.JournalInfo{
			JournalSize: expectedSum,
		})
	}
}

func initTestS3() (storage.Folder, internal.Uploader) {
	root := memory.NewFolder("", memory.NewKVS())
	mockUploader := internal.NewRegularUploader(
		&testtools.MockCompressor{},
		root.GetSubFolder(utility.WalPath),
	)
	return root, mockUploader
}

func toJournalNumber(index int) string {
	return fmt.Sprintf(JournalFmt, index)
}

func generateAndUploadData(mockUploader internal.Uploader, recordCount, recordSize int) {
	record := strings.Repeat("a", recordSize)
	// numerate journal names from 1 to make "MinimalJournalNumber" the minimal journal
	for i := 1; i <= recordCount; i++ {
		journalName := fmt.Sprintf(JournalFmt, i)

		r := bytes.NewReader([]byte(record))
		mockUploader.UploadFile(context.Background(), ioextensions.NewNamedReaderImpl(r, journalName))
	}
}

func createEmptySentinel(t *testing.T, root storage.Folder) {
	err := internal.UploadBackupInfo(root, SentinelName, internal.JournalInfo{})
	assert.NoError(t, err)
}

func TestEmptyFolder(t *testing.T) {
	recordCount := 0
	recordSize := 0
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(0)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOneJournal(t *testing.T) {
	recordCount := 1
	recordSize := 8
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(8)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestManyJournals(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(800)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleFrom(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := toJournalNumber(2)
	end := MaximalJournalNumber
	expectedSize := int64(8)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleTo(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(2)
	expectedSize := int64(16)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfFrom(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := toJournalNumber(50)
	end := MaximalJournalNumber
	expectedSize := int64(400)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfTo(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(50)
	expectedSize := int64(400)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheRightSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := toJournalNumber(100)
	end := MaximalJournalNumber
	expectedSize := int64(0)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheLeftSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := MinimalJournalNumber
	expectedSize := int64(0)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOnlyFirstRecord(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(1)
	expectedSize := int64(8)

	GenerateDataAndTest(t, recordCount, recordSize, begin, end, expectedSize)
}
