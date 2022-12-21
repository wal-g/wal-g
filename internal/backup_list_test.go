package internal_test

import (
	"bytes"
	"encoding/json"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"testing"
	"time"

	"github.com/wal-g/wal-g/utility"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

type MockGenericMetaInteractor struct{}

func (m *MockGenericMetaInteractor) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	return internal.GenericMetadata{}, nil
}

func (m *MockGenericMetaInteractor) SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error {
	return nil
}

func (m *MockGenericMetaInteractor) SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error {
	return nil
}

func TestBackupListFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	internal.DefaultHandleBackupList(folder.GetSubFolder(utility.BaseBackupPath), &MockGenericMetaInteractor{}, false, false)
}

var backups = []internal.BackupTime{
	{
		BackupName:  "base_123",
		Time:        time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
	{
		BackupName:  "base_456",
		Time:        time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
}

func TestBackupListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name     modified             wal_segment_backup_start\n" +
		"base_456 2018-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_123 2019-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeSlices(backups)
	internal.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| # | NAME     | MODIFIED                         | WAL SEGMENT BACKUP START |\n" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| 0 | base_456 | Thursday, 05-Jul-18 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_123 | Thursday, 25-Apr-19 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+----------+----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeSlices(backups)
	internal.WritePrettyBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTime
	buf := new(bytes.Buffer)

	err := internal.WriteAsJSON(backups, buf, false)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, backups)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"base_456\",\n" +
		"        \"time\": \"2018-07-05T01:01:50Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_123\",\n" +
		"        \"time\": \"2019-04-25T14:48:00Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    }\n" +
		"]"
	var unmarshaledBackups []internal.BackupTime
	buf := new(bytes.Buffer)

	internal.SortBackupTimeSlices(backups)
	err := internal.WriteAsJSON(backups, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshaledBackups)

	assert.NoError(t, err)
	assert.Equal(t, unmarshaledBackups, backups)
	assert.Equal(t, buf.String(), expectedString)
}

var backupsWithMeta = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			BackupName:  "base_123",
			Time:        time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		StartTime: time.Date(2017, 4, 17, 16, 49, 0, 0, time.UTC),
	},
	{
		BackupTime: internal.BackupTime{
			BackupName:  "base_456",
			Time:        time.Date(2020, 7, 5, 1, 1, 50, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		StartTime: time.Date(2016, 4, 17, 16, 49, 0, 0, time.UTC),
	},
}

func TestBackupWithMetaListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name     modified             wal_segment_backup_start\n" +
		"base_456 2020-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_123 2019-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(backupsWithMeta)

	sortedBackups := make([]internal.BackupTime, len(backupsWithMeta))
	for i, backupWithMeta := range backupsWithMeta {
		sortedBackups[i] = backupWithMeta.BackupTime
	}

	internal.WriteBackupList(sortedBackups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupWithMetaListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| # | NAME     | MODIFIED                         | WAL SEGMENT BACKUP START |\n" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| 0 | base_456 | Sunday, 05-Jul-20 01:01:50 UTC   | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_123 | Thursday, 25-Apr-19 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+----------+----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(backupsWithMeta)

	sortedBackups := make([]internal.BackupTime, len(backupsWithMeta))
	for i, backupWithMeta := range backupsWithMeta {
		sortedBackups[i] = backupWithMeta.BackupTime
	}

	internal.WritePrettyBackupList(sortedBackups, buf)
	assert.Equal(t, buf.String(), expected)
}
