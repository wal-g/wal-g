package internal_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

func TestBackupListFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	internal.DefaultHandleBackupList(folder)
}

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	internal.HandleBackupListWithFlags(folder, true, false, false)
}

var backups = []internal.BackupTime{
	{
		BackupName:       "base_123",
		ModificationTime: time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
		WalFileName:      "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
	{
		BackupName:       "base_456",
		ModificationTime: time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
		WalFileName:      "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
}

func TestBackupListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name     created modified             wal_segment_backup_start\n" +
		"base_456 -       2018-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_123 -       2019-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+----------+---------+----------------------------------+--------------------------+\n" +
		"| # | NAME     | CREATED | MODIFIED                         | WAL SEGMENT BACKUP START |\n" +
		"+---+----------+---------+----------------------------------+--------------------------+\n" +
		"| 0 | base_123 | -       | Thursday, 25-Apr-19 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_456 | -       | Thursday, 05-Jul-18 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+----------+---------+----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.WritePrettyBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

var backupsWithMetaData = []internal.BackupTime{
	{
		BackupName:       "base_123",
		CreationTime:     time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
		ModificationTime: time.Date(2021, 12, 2, 12, 52, 0, 1, time.UTC),
		WalFileName:      "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
	{
		BackupName:       "base_456",
		CreationTime:     time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
		ModificationTime: time.Date(2022, 8, 9, 2, 1, 10, 0, time.UTC),
		WalFileName:      "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
	{
		BackupName:       "base_789",
		CreationTime:     time.Date(2020, 6, 4, 2, 3, 32, 0, time.UTC),
		ModificationTime: time.Date(2020, 5, 1, 2, 3, 41, 0, time.UTC),
		WalFileName:      "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
}

func TestBackupListCorrectCreationTimeOutput(t *testing.T) {
	const expected = "" +
	"name     created              modified             wal_segment_backup_start\n" +
	"base_789 2020-06-04T02:03:32Z 2020-05-01T02:03:41Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
	"base_456 2018-07-05T01:01:50Z 2022-08-09T02:01:10Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
	"base_123 2019-04-25T14:48:00Z 2021-12-02T12:52:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.WriteBackupList(backupsWithMetaData, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTime
	buf := new(bytes.Buffer)

	err := internal.WriteAsJson(backups, buf, false)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, backups)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"base_123\",\n" +
		"        \"creation_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"modification_time\": \"2019-04-25T14:48:00Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_456\",\n" +
		"        \"creation_time\": \"0001-01-01T00:00:00Z\",\n" +
		"        \"modification_time\": \"2018-07-05T01:01:50Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    }\n" +
		"]"
	var unmarshaledBackups []internal.BackupTime
	buf := new(bytes.Buffer)

	err := internal.WriteAsJson(backups, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshaledBackups)

	assert.NoError(t, err)
	assert.Equal(t, unmarshaledBackups, backups)
	assert.Equal(t, buf.String(), expectedString)
}
