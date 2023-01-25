package greenplum_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

var restorePoints = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			BackupName:  "restore_123",
			Time:        time.Date(2019, 4, 25, 14, 48, 0, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		GenericMetadata: internal.GenericMetadata{
			StartTime: time.Date(2018, 4, 25, 14, 48, 0, 0, time.UTC),
		},
	},
	{
		BackupTime: internal.BackupTime{
			BackupName:  "restore_456",
			Time:        time.Date(2018, 7, 5, 1, 1, 50, 0, time.UTC),
			WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
		},
		GenericMetadata: internal.GenericMetadata{
			StartTime: time.Date(2017, 7, 5, 1, 1, 50, 0, time.UTC),
		},
	},
}

func TestRestorePointListCorrectOutput(t *testing.T) {
	const expected = "" +
		"name        created              wal_segment_backup_start\n" +
		"restore_456 2017-07-05T01:01:50Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n" +
		"restore_123 2018-04-25T14:48:00Z ZZZZZZZZZZZZZZZZZZZZZZZZ\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(restorePoints)
	internal.WriteBackupList(restorePoints, buf)
	assert.Equal(t, expected, buf.String())
}

func TestRestorePointListCorrectPrettyOutput(t *testing.T) {
	const expected = "" +
		"+---+-------------+-----------------------------------+--------------------------+\n" +
		"| # | NAME        | CREATED                           | WAL SEGMENT BACKUP START |\n" +
		"+---+-------------+-----------------------------------+--------------------------+\n" +
		"| 0 | restore_456 | Wednesday, 05-Jul-17 01:01:50 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"| 1 | restore_123 | Wednesday, 25-Apr-18 14:48:00 UTC | ZZZZZZZZZZZZZZZZZZZZZZZZ |\n" +
		"+---+-------------+-----------------------------------+--------------------------+\n"

	buf := new(bytes.Buffer)
	internal.SortBackupTimeWithMetadataSlices(restorePoints)
	internal.WritePrettyBackupList(restorePoints, buf)
	assert.Equal(t, expected, buf.String())
}

func TestRestorePointListCorrectJsonOutput(t *testing.T) {
	var actual []internal.BackupTimeWithMetadata
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

	internal.SortBackupTimeWithMetadataSlices(restorePoints)

	backupTimes := make([]internal.BackupTime, len(restorePoints))
	for i := 0; i < len(restorePoints); i++ {
		backupTimes[i] = restorePoints[i].BackupTime
	}

	err := internal.WriteAsJSON(backupTimes, buf, true)
	assert.NoError(t, err)
	err = json.Unmarshal(buf.Bytes(), &unmarshalledRestorePoints)

	assert.NoError(t, err)
	assert.Equal(t, unmarshalledRestorePoints, backupTimes)
	assert.Equal(t, buf.String(), expectedString)
}
