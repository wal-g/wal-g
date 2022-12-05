package redis

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"testing"
)

var emptyColumnsBackups = []archive.Backup{
	{
		BackupName: "backupName",
	},
	{
		UserData: "userData",
	},
}

var shortValuesBackups = []archive.Backup{
	{
		BackupName: "b1",
		UserData:   "u1",
	},
	{
		BackupName: "b2",
		UserData:   "u2",
	},
}

var longValuesBackups = []archive.Backup{
	{
		BackupName: "veryLongBackupName1",
		UserData:   "someLongUsefulUserData1",
	},
	{
		BackupName: "veryLongBackupName2",
		UserData:   "someLongUsefulUserData2",
	},
}

func TestWriteBackupListDetails_NoBackups(t *testing.T) {
	expectedOutput := "name start_time finish_time user_data data_size backup_size permanent\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(make([]archive.Backup, 0), &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_EmptyColumnsValues(t *testing.T) {
	expectedOutput := "name       start_time           finish_time          user_data data_size backup_size permanent\n" +
		"backupName 0001-01-01T00:00:00Z 0001-01-01T00:00:00Z <nil>     0         0           false\n" +
		"           0001-01-01T00:00:00Z 0001-01-01T00:00:00Z userData  0         0           false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(emptyColumnsBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_ShortColumnsValues(t *testing.T) {
	expectedOutput := "name start_time           finish_time          user_data data_size backup_size permanent\n" +
		"b1   0001-01-01T00:00:00Z 0001-01-01T00:00:00Z u1        0         0           false\n" +
		"b2   0001-01-01T00:00:00Z 0001-01-01T00:00:00Z u2        0         0           false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(shortValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_LongColumnsValues(t *testing.T) {
	expectedOutput := "name                start_time           finish_time          user_data               data_size backup_size permanent\n" +
		"veryLongBackupName1 0001-01-01T00:00:00Z 0001-01-01T00:00:00Z someLongUsefulUserData1 0         0           false\n" +
		"veryLongBackupName2 0001-01-01T00:00:00Z 0001-01-01T00:00:00Z someLongUsefulUserData2 0         0           false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(longValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}
