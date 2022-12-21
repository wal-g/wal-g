package greenplum_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

var restorePoints = []internal.BackupTime{
	{
		BackupName:  "restore_123",
		Time:        time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
	{
		BackupName:  "restore_456",
		Time:        time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	},
}

func TestRestorePointListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name        modified             wal_segment_backup_start\n" +
		"restore_456 2018-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"restore_123 2019-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeSlices(restorePoints)
	internal.WriteBackupList(restorePoints, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestRestorePointListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+-------------+----------------------------------+--------------------------+\n" +
		"| # | NAME        | MODIFIED                         | WAL SEGMENT BACKUP START |\n" +
		"+---+-------------+----------------------------------+--------------------------+\n" +
		"| 0 | restore_456 | Thursday, 05-Jul-18 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | restore_123 | Thursday, 25-Apr-19 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+-------------+----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeSlices(restorePoints)
	internal.WritePrettyBackupList(restorePoints, buf)
	assert.Equal(t, buf.String(), expected)
}

func TestRestorePointListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTime
	buf := new(bytes.Buffer)

	err := internal.WriteAsJSON(restorePoints, buf, false)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, actual, restorePoints)
}

func TestRestorePointListCorrectPrettyJsonOutput(t *testing.T) {
	const expectedString = "[\n" +
		"    {\n" +
		"        \"backup_name\": \"restore_456\",\n" +
		"        \"time\": \"2018-07-05T01:01:50Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    },\n" +
		"    {\n" +
		"        \"backup_name\": \"restore_123\",\n" +
		"        \"time\": \"2019-04-25T14:48:00Z\",\n" +
		"        \"wal_file_name\": \"ZZZZZZZZZZZZZZZZZZZZZZZZ\"\n" +
		"    }\n" +
		"]"
	var unmarshalledRestorePoints []internal.BackupTime
	buf := new(bytes.Buffer)

	internal.SortBackupTimeSlices(restorePoints)
	err := internal.WriteAsJSON(restorePoints, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshalledRestorePoints)

	assert.NoError(t, err)
	assert.Equal(t, unmarshalledRestorePoints, restorePoints)
	assert.Equal(t, buf.String(), expectedString)
}
