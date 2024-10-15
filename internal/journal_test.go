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
	SentinelFilePath     = "sentinel"
	MinimalJournalNumber = "000000000"
	MaximalJournalNumber = "999999999"
)

func GenerateS3SetAndTest(t *testing.T, recordCount, recordSize int, beginJournalID, endJournalID string, expectedSum int64) {
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

func GenerateS3SetAndAddJournalToOldBackup(t *testing.T, recordCount, recordSize int, beginJournalID, endJournalID string, expectedSum int64) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	createEmptySentinel(t, root)

	backupInfo, err := internal.GetBackupInfo(root, SentinelFilePath)
	assert.Error(t, err)
	assert.Equal(t, backupInfo, internal.BackupAndJournalInfo{})

	backupInfo.JournalSize, err = internal.GetJournalSizeInSemiInterval(
		root,
		utility.WalPath,
		func(a, b string) bool {
			return a < b
		},
		beginJournalID,
		endJournalID,
	)
	err = internal.UploadBackupInfo(root, SentinelFilePath, backupInfo)
	assert.NoError(t, err)

	backupInfo, err = internal.GetBackupInfo(root, SentinelFilePath)
	assert.NoError(t, err)
	assert.Equal(t, backupInfo, internal.BackupAndJournalInfo{
		JournalSize: int64(expectedSum),
	})
}

func initTestS3() (*memory.Folder, internal.Uploader) {
	storage := memory.NewKVS()
	root := memory.NewFolder("", storage)
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
	err := root.PutObject(SentinelFilePath, bytes.NewReader([]byte("{}")))
	assert.NoError(t, err)
}

func TestEmptyFolder(t *testing.T) {
	recordCount := 0
	recordSize := 0
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(0)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOneJournal(t *testing.T) {
	recordCount := 1
	recordSize := 8
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(8)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestManyJournals(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := MaximalJournalNumber
	expectedSize := int64(800)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleFrom(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := toJournalNumber(2)
	end := MaximalJournalNumber
	expectedSize := int64(8)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleTo(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(2)
	expectedSize := int64(16)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfFrom(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := toJournalNumber(50)
	end := MaximalJournalNumber
	expectedSize := int64(400)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfTo(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(50)
	expectedSize := int64(400)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheRightSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := toJournalNumber(100)
	end := MaximalJournalNumber
	expectedSize := int64(0)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheLeftSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := MinimalJournalNumber
	expectedSize := int64(0)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOnlyFirstRecord(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := MinimalJournalNumber
	end := toJournalNumber(1)
	expectedSize := int64(8)

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}
