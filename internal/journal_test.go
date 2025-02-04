package internal_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

var (
	JournalFmt              = "%09d"
	BackupName              = "stream"
	MinimalJournalNumber    = time.Now()
	DefaultJournalDirectory = "journals_005"
	journalTimestamps       = map[int]time.Time{}
)

func NewEmptyTestJournal(
	JournalName string,
	start, end string,
) internal.JournalInfo {
	return internal.JournalInfo{
		JournalDirectoryName: DefaultJournalDirectory,
	}
}

func initTestS3() (storage.Folder, internal.Uploader) {
	root := memory.NewFolder("", memory.NewKVS())
	mockUploader := internal.NewRegularUploader(
		&testtools.MockCompressor{},
		root,
	)
	return root, mockUploader
}

func numberToJournalTimestamp(num int) time.Time {
	return journalTimestamps[num]
}

func generateAndUploadData(t *testing.T, mockUploader internal.Uploader) {
	recordCount := 100
	recordSize := 1

	record := strings.Repeat("a", recordSize)
	for i := 1; i <= recordCount; i++ {
		journalPath := fmt.Sprintf("%s/"+JournalFmt, DefaultJournalDirectory, i)

		r := bytes.NewReader([]byte(record))
		err := mockUploader.Upload(context.Background(), journalPath, r)
		assert.NoError(t, err)

		time.Sleep(time.Millisecond)
	}

	objs, _, err := mockUploader.Folder().GetSubFolder(DefaultJournalDirectory).ListFolder()
	assert.NoError(t, err)
	for _, obj := range objs {
		value, err := strconv.Atoi(obj.GetName())
		assert.NoError(t, err)

		journalTimestamps[value] = obj.GetLastModified()
	}
}

func CreateThreeJournals(
	t *testing.T,
	folder storage.Folder,
) (internal.JournalInfo, internal.JournalInfo, internal.JournalInfo) {
	ji1 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*5).
				Format(internal.JournalTimeLayout),
		),
		MinimalJournalNumber, MinimalJournalNumber,
		DefaultJournalDirectory,
	)
	assert.NoError(t, ji1.Upload(folder))

	ji2 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*10).
				Format(internal.JournalTimeLayout),
		),
		MinimalJournalNumber, numberToJournalTimestamp(33),
		DefaultJournalDirectory,
	)

	assert.NoError(t, ji2.Upload(folder))
	assert.NoError(t, ji2.UpdateIntervalSize(folder))
	assert.NoError(t, ji1.Read(folder))

	ji3 := internal.NewEmptyJournalInfo(
		fmt.Sprintf(
			"%s_%s",
			BackupName,
			time.
				Now().
				Add(time.Second*15).
				Format(internal.JournalTimeLayout),
		),
		numberToJournalTimestamp(33), numberToJournalTimestamp(66),
		DefaultJournalDirectory,
	)

	assert.NoError(t, ji3.Upload(folder))
	assert.NoError(t, ji3.UpdateIntervalSize(folder))
	assert.NoError(t, ji2.Read(folder))
	assert.NoError(t, ji1.Read(folder))

	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)

	return ji1, ji2, ji3
}

func TestCreateThreeJournals(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	CreateThreeJournals(t, folder)
}

func TestDeleteJournalInMiddle(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji2.Delete(folder))
	assert.NoError(t, ji1.Read(folder))
	assert.NoError(t, ji3.Read(folder))
	assert.Equal(t, int64(66), ji1.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}

func TestDeleteJournalInBegin(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji1.Delete(folder))
	assert.NoError(t, ji2.Read(folder))
	assert.NoError(t, ji3.Read(folder))
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}

func TestDeleteJournalInEnd(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	assert.NoError(t, ji3.Delete(folder))
	assert.NoError(t, ji1.Read(folder))
	assert.NoError(t, ji2.Read(folder))
	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(0), ji2.SizeToNextBackup)
	fmt.Println(ji1.JournalName, ji2.JournalName, ji3.JournalName)
}

func TestSafetyOfRepeatingMethodCalls(t *testing.T) {
	folder, uploader := initTestS3()
	generateAndUploadData(t, uploader)

	ji1, ji2, ji3 := CreateThreeJournals(t, folder)

	// There are random method calls
	for i := 0; i < 10; i++ {
		assert.NoError(t, ji1.UpdateIntervalSize(folder))
		assert.NoError(t, ji1.Upload(folder))
		assert.NoError(t, ji3.Read(folder))
		assert.NoError(t, ji2.Upload(folder))
		assert.NoError(t, ji2.UpdateIntervalSize(folder))
		assert.NoError(t, ji3.Upload(folder))
		assert.NoError(t, ji3.UpdateIntervalSize(folder))
		assert.NoError(t, ji2.Read(folder))
		assert.NoError(t, ji1.Read(folder))
	}

	assert.Equal(t, int64(33), ji1.SizeToNextBackup)
	assert.Equal(t, int64(33), ji2.SizeToNextBackup)
	assert.Equal(t, int64(0), ji3.SizeToNextBackup)
}
