package internal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

var (
	ZeroDate         = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
	FutureDate       = time.Now().Add(time.Hour * 8760)
	SentinelFilePath = "sentinel"
)

func GenerateS3SetAndTest(t *testing.T, recordCount, recordSize int, beginTimeFunc, endTimeFunc func([]storage.Object) time.Time, expected int) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	start, end := getTimeIntervalByData(t, root, beginTimeFunc, endTimeFunc)

	journalSize, err := internal.GetJournalSizeInSemiInterval(root, utility.WalPath, start, end)
	assert.NoError(t, err)
	assert.Equal(t, int64(expected), journalSize)
}

func GenerateS3SetAndAddJournalToOldBackup(t *testing.T, recordCount, recordSize int, beginTimeFunc, endTimeFunc func([]storage.Object) time.Time, expected int) {
	root, mockUploader := initTestS3()

	generateAndUploadData(mockUploader, recordCount, recordSize)

	start, end := getTimeIntervalByData(t, root, beginTimeFunc, endTimeFunc)

	createEmptySentinel(t, root)

	err := internal.AddJournalSizeToPreviousBackup(root, utility.WalPath, "", SentinelFilePath, start, end)
	assert.NoError(t, err)

	journalSize := readJournalSizeFromSentinel(t, root)
	assert.Equal(t, int64(expected), journalSize)
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

func generateAndUploadData(mockUploader internal.Uploader, recordCount, recordSize int) {
	record := strings.Repeat("a", recordSize)
	for i := 0; i < recordCount; i++ {
		journalName := fmt.Sprintf("%d", i)

		r := bytes.NewReader([]byte(record))
		mockUploader.UploadFile(context.Background(), ioextensions.NewNamedReaderImpl(r, journalName))
	}
}

func createEmptySentinel(t *testing.T, root storage.Folder) {
	err := root.PutObject(SentinelFilePath, bytes.NewReader([]byte("{}")))
	assert.NoError(t, err)
}

func readJournalSizeFromSentinel(t *testing.T, root storage.Folder) int64 {
	var sentinel map[string]json.RawMessage
	sentinelReader, err := root.ReadObject(SentinelFilePath)
	assert.NoError(t, err)

	rawSentinel, err := io.ReadAll(sentinelReader)
	assert.NoError(t, err)

	err = json.Unmarshal(rawSentinel, &sentinel)
	assert.NoError(t, err)

	journalSize, err := strconv.ParseInt(string(sentinel[internal.JournalSize]), 10, 64)
	assert.NoError(t, err)

	return journalSize
}

func getTimeIntervalByData(t *testing.T, root *memory.Folder, beginTimeFunc, endTimeFunc func([]storage.Object) time.Time) (time.Time, time.Time) {
	jfiles, _, err := root.GetSubFolder(utility.WalPath).ListFolder()
	assert.NoError(t, err)

	sort.Slice(jfiles, func(i, j int) bool {
		return jfiles[i].GetLastModified().Before(jfiles[j].GetLastModified())
	})

	return beginTimeFunc(jfiles), endTimeFunc(jfiles)
}

func TestEmptyFolder(t *testing.T) {
	recordCount := 0
	recordSize := 0
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 0

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOneJournal(t *testing.T) {
	recordCount := 1
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 8

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestManyJournals(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 800

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleFrom(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := func(jfiles []storage.Object) time.Time { return jfiles[1].GetLastModified() }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 8

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestSimpleTo(t *testing.T) {
	recordCount := 3
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func(jfiles []storage.Object) time.Time { return jfiles[1].GetLastModified() }
	expectedSize := 16

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfFrom(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func(jfiles []storage.Object) time.Time { return jfiles[49].GetLastModified() }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 400

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestHalfTo(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func(jfiles []storage.Object) time.Time { return jfiles[49].GetLastModified() }
	expectedSize := 400

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheRightSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func(jfiles []storage.Object) time.Time { return jfiles[99].GetLastModified() }
	end := func([]storage.Object) time.Time { return FutureDate }
	expectedSize := 0

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestEmptySetOnTheLeftSide(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func(jfiles []storage.Object) time.Time { return jfiles[0].GetLastModified().Add(-time.Microsecond) }
	expectedSize := 0

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}

func TestOnlyFirstRecord(t *testing.T) {
	recordCount := 100
	recordSize := 8
	begin := func([]storage.Object) time.Time { return ZeroDate }
	end := func(jfiles []storage.Object) time.Time { return jfiles[0].GetLastModified() }
	expectedSize := 8

	GenerateS3SetAndTest(t, recordCount, recordSize, begin, end, expectedSize)
	GenerateS3SetAndAddJournalToOldBackup(t, recordCount, recordSize, begin, end, expectedSize)
}
