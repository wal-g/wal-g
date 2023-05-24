package internal_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/wal-g/wal-g/utility"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	internal.DefaultHandleBackupList(folder.GetSubFolder(utility.BaseBackupPath), &testtools.MockGenericMetaFetcher{}, false, false)
}

var backups = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			BackupName:  "base_123",
			Time:        time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		GenericMetadata: internal.GenericMetadata{
			StartTime: time.Date(2018, 4, 25, 14, 48, 0, 0, time.UTC),
		},
	},
	{
		BackupTime: internal.BackupTime{
			BackupName:  "base_456",
			Time:        time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		GenericMetadata: internal.GenericMetadata{
			StartTime: time.Date(2017, 7, 5, 1, 1, 50, 0, time.UTC),
		},
	},
}

func TestBackupListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name     created              wal_segment_backup_start\n" +
		"base_456 2017-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_123 2018-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(backups)
	internal.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+----------+-----------------------------------+--------------------------+\n" +
		"| # | NAME     | CREATED                           | WAL SEGMENT BACKUP START |\n" +
		"+---+----------+-----------------------------------+--------------------------+\n" +
		"| 0 | base_456 | Wednesday, 05-Jul-17 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_123 | Wednesday, 25-Apr-18 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+----------+-----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(backups)
	internal.WritePrettyBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTimeWithMetadata
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
	var unmarshalledBackups []internal.BackupTime
	buf := new(bytes.Buffer)

	internal.SortBackupTimeWithMetadataSlices(backups)

	backupTimes := make([]internal.BackupTime, len(backups))
	for i := 0; i < len(backups); i++ {
		backupTimes[i] = backups[i].BackupTime
	}

	err := internal.WriteAsJSON(backupTimes, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshalledBackups)

	assert.NoError(t, err)
	assert.Equal(t, unmarshalledBackups, backupTimes)
	assert.Equal(t, expectedString, buf.String())
}
