package test

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
	internal.HandleBackupList(folder)
}

func TestBackupListFlagsFindsBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	internal.HandleBackupListWithFlags(folder, true, false, false)
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
		"name     last_modified        wal_segment_backup_start\n" +
		"base_456 2018-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"base_123 2019-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.WriteBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| # | NAME     | LAST MODIFIED                    | WAL SEGMENT BACKUP START |\n" +
		"+---+----------+----------------------------------+--------------------------+\n" +
		"| 0 | base_123 | Thursday, 25-Apr-19 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | base_456 | Thursday, 05-Jul-18 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+----------+----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.WritePrettyBackupList(backups, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestBackupListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTime
	buf := new(bytes.Buffer)

	internal.WriteBackupListAsJson(backups, buf, false)
	err := json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, backups)
}

func TestBackupListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"base_123\",\n" +
		"        \"time\": \"2019-04-25T14:48:00Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"base_456\",\n" +
		"        \"time\": \"2018-07-05T01:01:50Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    }\n" +
		"]"
	var unmarshaledBackups []internal.BackupTime
	buf := new(bytes.Buffer)

	internal.WriteBackupListAsJson(backups, buf, true)
	err := json.Unmarshal(buf.Bytes(), &unmarshaledBackups)

	assert.NoError(t, err)
	assert.Equal(t, unmarshaledBackups, backups)
	assert.Equal(t, buf.String(), expectedString)
}
